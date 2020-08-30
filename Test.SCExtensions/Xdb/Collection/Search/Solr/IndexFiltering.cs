using System.Collections.Generic;
using System.Linq;

using Microsoft.Extensions.Logging;

using Sitecore.Xdb.Collection.Indexing;
using Sitecore.Xdb.Collection.Model;

namespace Test.SCExtensions.Xdb.Collection.Search.Solr
{
    public class IndexFiltering
    {
        //Example filtering based on page event id eeaac6d5-f3b8-4616-a05e-c357fddc5050
        public static string[] AllowedEventIds => new[] { "eeaac6d5-f3b8-4616-a05e-c357fddc5050" };
       
        public static List<InteractionDataRecord> FilteredRecords(IReadOnlyCollection<InteractionDataRecord> records, ILogger<SingleThreadedIndexer> logger)
        {
            var filteredRecords = new List<InteractionDataRecord>();

            foreach (var interaction in records)
            {
                foreach (var jToken in interaction.Events)
                {
                    //Flatten object to allow string search
                    var eventInfo = jToken.ToString();
                    
                    if (AllowedEventIds.Any(eventInfo.Contains))
                    {
                        filteredRecords.Add(interaction);
                    }
                    else
                    {
                        logger.LogInformation(0, $"[{nameof(IndexFiltering)}] Interaction with id:{interaction.Id} skipped from indexing.");
                        break;
                    }
                }
            }

            return filteredRecords;
        }
    }
}