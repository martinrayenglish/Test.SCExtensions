using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Sitecore.Framework.Conditions;
using Sitecore.Xdb.Collection;
using Sitecore.Xdb.Collection.Indexing;

namespace Test.SCExtensions.Xdb.Collection.Search.Solr
{
    public sealed class IndexRebuilderFilterDecorator : IIndexRebuilder
    {
        private readonly IIndexRebuilder _inner;
        private readonly ILogger<SingleThreadedIndexer> _logger;

        public IndexRebuilderFilterDecorator(IIndexRebuilder inner, ILogger<SingleThreadedIndexer> logger)
        {
            _inner = inner;
            _logger = Condition.Requires(logger, nameof(logger)).IsNotNull().Value;
        }

        public Task BeginRebuild()
        {
            return _inner.BeginRebuild();
        }

        public Task CompleteRebuild(ISyncToken syncToken)
        {
            return _inner.CompleteRebuild(syncToken);
        }

        public async Task WriteDataBatch(ChangedDataRecords changedDataRecordsBatch)
        {
            var filteredRecords = IndexFiltering.FilteredRecords(changedDataRecordsBatch.InteractionRecords, _logger);
            var filteredChanges = new ChangedDataRecords(changedDataRecordsBatch.ContactRecords, filteredRecords.AsReadOnly(), changedDataRecordsBatch.DeletedContactIds, changedDataRecordsBatch.DeletedInteractionIds);
            
            await _inner.WriteDataBatch(filteredChanges).ConfigureAwait(false);
        }
    }
}