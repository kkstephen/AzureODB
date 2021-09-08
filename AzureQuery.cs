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
        
        public string Token { get; set; }
        public string PartId { get; set; }
        public int MaxItems { get; set; }

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
            this.MaxItems = -1;
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
                this.container = null;
                
                if (this.Items != null)
                {
                    this.Items.Clear();
                }

                this.Items = null;

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

            this.reader = this.container.GetItemQueryIterator<T>(qdef, this.Token, this.Options);
            
            await this.getData();

            return this.Items;
        }

        public async Task<IList<T>> Get(Expression<Func<T, bool>> pred)
        {  
            this.reader = this.container.GetItemLinqQueryable<T>(true, this.Token, this.Options).Where<T>(pred).ToFeedIterator(); 
            
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

        public async Task<int> Count()
        {
            QueryDefinition qdef = new QueryDefinition("SELECT VALUE COUNT(1) FROM c");

            using (FeedIterator<int> iterator = this.container.GetItemQueryIterator<int>(qdef))
            {
                FeedResponse<int> rs = await iterator.ReadNextAsync();

                return rs.AsEnumerable().First();
            }
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
