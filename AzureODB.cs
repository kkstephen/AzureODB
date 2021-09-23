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

        private bool disposed = false;

        public AzureODB(IConfiguration config)
        {
            var connStr = config.GetSection("cosmos");

            uri = connStr.GetSection("uri").Value;
            key = connStr.GetSection("key").Value;
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
                }

                this.cosmos = null;

                this.disposed = true;
            }
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }
         
        public AzureQuery<T> Open<T>(string dbname, string collection, string partitionKey)
        {
            this.cosmos = new CosmosClient(this.uri, this.key, new CosmosClientOptions() { ApplicationName = "CosmosDB" });

            var cm = this.cosmos.GetDatabase(dbname);
            var ct = cm.GetContainer(collection); 
                        
            return new AzureQuery<T>(ct) { PartId = partitionKey };
        } 
    }
}
