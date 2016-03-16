using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using Microsoft.Isam.Esent.Interop;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Database.Storage;
using Raven.Imports.Newtonsoft.Json;
using Raven.Storage.Esent;
using Raven.Storage.Esent.StorageActions;
using TransactionalStorage = Raven.Storage.Esent.TransactionalStorage;

namespace Raven.Bundles.DebugExtentions
{
	public class DocumentStorageStatsResponders : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/debug/extremely-slow-top-big-documents?$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new string[] {"GET"}; }
		}

		public class DocumentStats
		{
			public string Name;
			public int Size;
		}

		private void ExtractInternalEsentProperties(IStorageActionsAccessor accessor, out Session session,
			out TableColumnsCache tableColumnsCache, out Table documentsTable)
		{
			var documentStrorageActions = accessor.Documents as DocumentStorageActions;

			session = documentStrorageActions.Session;

			var tableColumnCacheField = typeof(DocumentStorageActions).GetField("tableColumnsCache", BindingFlags.NonPublic|BindingFlags.Instance);
			tableColumnsCache = tableColumnCacheField.GetValue(documentStrorageActions) as TableColumnsCache;

			var documentsTableField = typeof(DocumentStorageActions).GetProperty("Documents", BindingFlags.NonPublic | BindingFlags.Instance);
			documentsTable = documentsTableField.GetValue(documentStrorageActions) as Table;
		}

		
		public override void Respond(IHttpContext context)
		{
			int minSizeToTrace;
			try
			{
				minSizeToTrace = Int32.Parse(context.Request.QueryString["minSizeInKB"])*1024;
			}
			catch (Exception e)
			{
				context.Write("minSizeInKB query parameter should be provided");
				return;
			}
			
			var transactionalStorage = Database.TransactionalStorage as TransactionalStorage;
			if (transactionalStorage == null)
			{
				context.Write("Only ESENT storage supported");
				return;
			}

			try
			{
				using (var streamingDisposer = context.Response.Streaming())
				using (var writer = new JsonTextWriter(new StreamWriter(context.Response.OutputStream)))

				{
					writer.WriteStartObject();
					writer.WritePropertyName("DocumentsAndSizes");
					writer.WriteStartArray();
					transactionalStorage.Batch(accessor =>
					{
						Session session;
						TableColumnsCache tableColumnsCache;
						Table documentsTable;

						ExtractInternalEsentProperties(accessor, out session, out tableColumnsCache, out documentsTable);

						Api.JetSetCurrentIndex(session, documentsTable, "by_etag");
						Api.MoveBeforeFirst(session, documentsTable);
						Stopwatch duration = null;
						var sp = Stopwatch.StartNew();
						while (Api.TryMoveNext(session, documentsTable))
						{
							var dataColumnSize = Api.RetrieveColumnSize(session, documentsTable, tableColumnsCache.DocumentsColumns["data"]);
							var metadataColumnSize = Api.RetrieveColumnSize(session, documentsTable,
								tableColumnsCache.DocumentsColumns["metadata"]);

							var curDocSize = dataColumnSize ?? 0 + metadataColumnSize ?? 0;
							if (sp.ElapsedMilliseconds > 1000)
							{
								context.Write(" ");
								sp.Restart();
							}
							if (curDocSize > minSizeToTrace)
							{
								writer.WriteStartObject();
								writer.WritePropertyName("Id");

								string docKey = Api.RetrieveColumnAsString(session, documentsTable, tableColumnsCache.DocumentsColumns["key"]);
								writer.WriteValue(docKey);
								writer.WritePropertyName("Size");
								writer.WriteValue(curDocSize);
								writer.WriteEndObject();
								writer.Flush();
							}
						}
					});
					writer.WriteEndArray();
					writer.WriteEndObject();
					writer.Flush();
					writer.Close();
				}
			}
			catch (Exception e)
			{
				context.Write(e.Message);
			}

		}
		
	}
}
