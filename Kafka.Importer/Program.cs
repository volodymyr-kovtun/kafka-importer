using Confluent.Kafka;
using System.CommandLine;

var boostrapServersOption = new Option<string>(name: "--bootstrap-servers", description: "Kafka bootstrap servers");
var topicNameOption = new Option<string>(name: "--topic", "Kafka topic to read data from");
var keyOption = new Option<string>(name: "--key", "Key to filter messages (optional)");
var detFolderOption = new Option<string?>(name: "--destFolder", "Destination folder to save messages");

var rootCommand = new RootCommand("Import messages from Kafka topic");
rootCommand.AddOption(boostrapServersOption);
rootCommand.AddOption(topicNameOption);
rootCommand.AddOption(keyOption);
rootCommand.AddOption(detFolderOption);

rootCommand.SetHandler((boostrapServers, topicName, key, destFolder) =>
{
    var destinationFolder = destFolder ?? topicName;
    
    if (!Directory.Exists(destinationFolder))
    {
        Directory.CreateDirectory(destinationFolder);
    }

    var config = new ConsumerConfig { BootstrapServers = boostrapServers, GroupId = "import-consumer" };
    using var adminClientBuilder = new AdminClientBuilder(config).Build();
    
    var consumerConfig = new ConsumerConfig(config)
    {
        EnablePartitionEof = true,
        EnableAutoCommit = false
    };

    int messageNumber = 1;
    int amountProcessedMessages = 0;
    
    using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

    try
    {
        var metadata = adminClientBuilder.GetMetadata(topicName, TimeSpan.FromSeconds(10));
        
        metadata.Topics.First(i => i.Topic == topicName).Partitions.ForEach(partition =>
        {
            consumer.Assign(new TopicPartitionOffset(topicName, new Partition(partition.PartitionId), Offset.Beginning));

            while (true)
            {
                var consumeResult = consumer.Consume();
                amountProcessedMessages++;

                if (amountProcessedMessages % 500 == 0)
                {
                    Console.WriteLine($"Processed {amountProcessedMessages} messages");
                }

                if (consumeResult == null || consumeResult.IsPartitionEOF)
                {
                    break;
                }

                if (!string.IsNullOrEmpty(key) && consumeResult.Message.Key != key)
                {
                    continue;
                }

                string filePath = Path.Combine(destinationFolder, $"{messageNumber}.json");
                File.WriteAllText(filePath, consumeResult.Message.Value);
                messageNumber++;
            }

        });
    }
    catch (Exception ex)
    {
        Console.WriteLine(ex);
        throw;
    }
    finally
    {
        consumer.Close();
    }
    
    Console.WriteLine($"Finish importing data. Count messages: '{messageNumber}'");
}, boostrapServersOption, topicNameOption, keyOption, detFolderOption);

return await rootCommand.InvokeAsync(args);