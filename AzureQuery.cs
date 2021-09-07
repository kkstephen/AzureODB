using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Linq;

namespace WebApplication1.Models
{
    public class AzureQuery<T> : IDisposable
    {   
        protected Container container;
        protected FeedIterator<T> reader;
        protected IList<T> Items;
        
        public string Token;
        public string PartId;
        public int MaxItems = -1;
        
        public QueryRequestOptions Options
        {
            get
            {
                QueryRequestOptions options = new QueryRequestOptions();

                if (!string.IsNullOrEmpty(this.PartId))
                {
                    options.PartitionKey = new PartitionKey(this.PartId);
                }

                options.MaxItemCount = this.MaxItems;

                return options;
            }
        }
        private bool disposed = false;
        
        public AzureQuery(Container container)
        {
            this.container = container;
        }
        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    if (this.reader != null)
                    {
                        this.reader.Dispose();
                    }
                }

                this.reader = null;

                this.disposed = true;
            }
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        } 

        public async Task<IList<T>> Get(string sql)
        {
            QueryDefinition qdef = new QueryDefinition(sql); 

            this.reader = container.GetItemQueryIterator<T>(qdef, this.Token, this.Options);
            
            await this.getData();

            return this.Items;
        }

        public async Task<IList<T>> Get(Expression<Func<T, bool>> pred)
        {  
            this.reader = container.GetItemLinqQueryable<T>(true, this.Token, this.Options).Where<T>(pred).ToFeedIterator(); 
            
            await this.getData();

            return this.Items;
        }

        public async Task<bool> Save(T t) 
        {
            ItemResponse<T> ret = await container.CreateItemAsync(t, new PartitionKey(this.PartId));

            return ret.StatusCode == System.Net.HttpStatusCode.OK || ret.StatusCode == System.Net.HttpStatusCode.Created;
        }

        public async Task Delete(string id)
        {
            await this.container.DeleteItemAsync<T>(id, new PartitionKey(this.PartId));
        }

        private async Task getData()
        {
            this.Items = new List<T>(); 
             
            FeedResponse<T> rs = await this.reader.ReadNextAsync();

            if (reader.HasMoreResults)
                this.Token = rs.ContinuationToken;
            
            foreach (T t in rs)
            {
                Items.Add(t);
            }  
        }
    }
}
