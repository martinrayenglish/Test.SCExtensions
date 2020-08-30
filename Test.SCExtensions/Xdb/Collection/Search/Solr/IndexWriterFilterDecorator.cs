using System.Threading;
using System.Threading.Tasks;

using Microsoft.Extensions.Logging;

using Sitecore.Framework.Conditions;
using Sitecore.Xdb.Collection.Indexing;

namespace Test.SCExtensions.Xdb.Collection.Search.Solr
{
    internal sealed class IndexWriterFilterDecorator : IIndexWriter
    {
        private readonly IIndexWriter _writer;
        private readonly ILogger<SingleThreadedIndexer> _logger;

        public IndexWriterFilterDecorator(IIndexWriter writer, ILogger<SingleThreadedIndexer> logger)
        {
            _writer = Condition.Requires(writer, nameof(writer)).IsNotNull().Value;
            _logger = Condition.Requires(logger, nameof(logger)).IsNotNull().Value;
        }

        public async Task<Task> Write(ChangedDataRecords changes, CancellationToken cancellationToken)
        {
            changes = Condition.Requires(changes, nameof(changes)).IsNotNull().Value;
            
            var filteredRecords = IndexFiltering.FilteredRecords(changes.InteractionRecords, _logger);
            ChangedDataRecords filteredChanges = new ChangedDataRecords(changes.ContactRecords, filteredRecords.AsReadOnly(), changes.DeletedContactIds, changes.DeletedInteractionIds);
            
            Task task = await _writer.Write(filteredChanges, cancellationToken).ConfigureAwait(false);
            return task;
        }

        public async Task SignalChangesHaveBeenWritten(byte[] token, CancellationToken cancellationToken)
        {
            await _writer.SignalChangesHaveBeenWritten(token, cancellationToken).ConfigureAwait(false);
        }
    }
}