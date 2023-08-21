using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Azure.Cosmos;

namespace WebApplication1.Models
{
    public class AzureODB : IDisposable
    {
		private CosmosClient cosmos;

		private string uri;
		private string key;
		private string db;

		public double Charge;

		private bool disposed = false;

		public Container Container { get; private set; }

		public AzureODB(IConfiguration config)
		{
			var connStr = config.GetSection("cosmos");

			this.uri = connStr.GetSection("uri").Value;
			this.key = connStr.GetSection("key").Value;

			this.db = connStr.GetSection("db").Value;

			this.Charge = 0;
		}

        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    if (this.cosmos != null)
                    {
                        this.cosmos.Dispose();
                    }
                    
                    this.cosmos = null;
                }
                
                this.Container = null;

                this.disposed = true;
            }
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }
         
        public AzureQuery<T> Open<T>(string collection, string partitionKey)
        {
            this.cosmos = new CosmosClient(this.uri, this.key, new CosmosClientOptions() { ApplicationName = "CosmosDB" });

            var cm = this.cosmos.GetDatabase(this.db);
            this.Container = cm.GetContainer(collection); 
                        
            return Query<T>(partitionKey);
        }

        public AzureQuery<T> Query<T>(string partitionKey)
        {
            return new AzureQuery<T>(this.Container) { PartId = partitionKey };
        }
        
        public async Task<bool> Set<T>(T t, string pkid)
        {
            ItemResponse<T> rs = await Container.UpsertItemAsync(t, new PartitionKey(pkid));

            return rs.StatusCode ==HttpStatusCode.OK || rs.StatusCode == HttpStatusCode.Created;
        }

        public async Task<int> Count(string sql)
        {
            QueryDefinition qdef = new QueryDefinition(sql);

            using (FeedIterator<int> iterator = this.Container.GetItemQueryIterator<int>(qdef))
            {
                FeedResponse<int> rs = await iterator.ReadNextAsync();

                this.Charge += rs.RequestCharge;

                return rs.AsEnumerable().First();
            }
        } 
    }
}
