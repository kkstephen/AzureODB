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
        protected FeedIterator<T> iterator;
        
        public string Token { get; set; }
        public string PartId { get; set; }
        public int MaxItems { get; set; }
        public double Charge { get; set; }

        public QueryRequestOptions Options
        {
            get
            {
                QueryRequestOptions options = new QueryRequestOptions();

                if (!string.IsNullOrEmpty(this.PartId))
                {
                    options.PartitionKey = new PartitionKey(this.PartId);
                }

                options.MaxBufferedItemCount = -1;
                options.MaxConcurrency = -1;
                options.MaxItemCount = this.MaxItems;

                return options;
            }
        }

        private bool disposed = false;
        
        public AzureQuery(Container container)
        {
            this.container = container;
            this.MaxItems = -1;

            this.Charge = 0;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!this.disposed)
            {
                if (disposing)
                {
                    if (this.iterator != null)
                    {
                        this.iterator.Dispose();
                    }
                    
                    this.iterator = null; 
                }              
               
                this.disposed = true;
            }
        }

        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        public async Task<T> Get(string id)
        {
            ItemResponse<T> item = await container.ReadItemAsync<T>(
                        id: id,
                partitionKey: new PartitionKey(PartId)
            );

            this.Charge = item.RequestCharge;
  
            return item.Resource; 
        }

        public async Task<T> Get(Expression<Func<T, bool>> pred)
        {
            var query = this.container.GetItemLinqQueryable<T>(true, this.Token, this.Options).Where<T>(pred);

            this.iterator = query.ToFeedIterator();

            FeedResponse<T> items = await iterator.ReadNextAsync();

            this.Charge = items.RequestCharge;

            return items.AsEnumerable().FirstOrDefault();
        } 
        
        public async Task<IList<T>> Gets(string sql)
        {
            QueryDefinition qdef = new QueryDefinition(sql);
            
            return await this.Gets(qdef);
        }

        public async Task<IList<T>> Gets(Expression<Func<T, bool>> pred)
        {
            var query = this.container.GetItemLinqQueryable<T>(true, this.Token, this.Options)
                           .Where<T>(pred);

            this.iterator = query.ToFeedIterator();

            return await this.read();
        }

        public async Task<IList<T>> Gets(QueryDefinition qdef)
        {
            this.iterator = this.container.GetItemQueryIterator<T>(qdef, this.Token, this.Options);

            return await this.read();
        }

        public async Task<IList<T>> GetParts(string sql, int page, int take)
        { 
            int start = (page - 1) * take;

            sql += " offset @row limit @size";
            
            QueryDefinition qdef = new QueryDefinition(sql).WithParameter("@row", start).WithParameter("@size", take);

            this.iterator = this.container.GetItemQueryIterator<T>(qdef, this.Token, this.Options);

            IList<T> list = new List<T>(); 

            if (iterator.HasMoreResults)
            {
                FeedResponse<T> rs = await this.iterator.ReadNextAsync(); 
              
                this.Charge = rs.RequestCharge;

                foreach (var item in rs)
                {
                    list.Add(item);
                }
            }

            return list; 
        } 

        public async Task<bool> Save(T t)
        {
            ItemResponse<T> rs = await container.UpsertItemAsync(t, new PartitionKey(this.PartId));

            return rs.StatusCode == HttpStatusCode.OK || rs.StatusCode == HttpStatusCode.Created;
        } 

        public async Task<bool> Delete(string id)
        {
            var rs = await this.container.DeleteItemAsync<T>(id, new PartitionKey(this.PartId));

            return rs.StatusCode == HttpStatusCode.OK;
        } 

        private async Task<IList<T>> read()
        { 
            IList<T> list = new List<T>();
           
            while (iterator.HasMoreResults)
            {
                FeedResponse<T> rs = await this.iterator.ReadNextAsync();

                foreach (var item in rs)
                {
                    list.Add(item);
                }

                this.Charge += rs.RequestCharge;
                this.Token = rs.ContinuationToken;

                if (list.Count >= this.MaxItems && this.MaxItems != -1)
                {
                    break;
                }
            } 

            return list;
         }
    }
}
