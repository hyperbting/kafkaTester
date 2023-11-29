using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using UnityEngine;

public class KafkaConnect : MonoBehaviour
{
    ConsumerConfig _config = new ConsumerConfig
    {
        GroupId = "test-consumer-group",
        BootstrapServers = "192.168.1.102:9092",
        // Note: The AutoOffsetReset property determines the start offset in the event
        // there are not yet any committed offsets for the consumer group for the
        // topic/partitions of interest. By default, offsets are committed
        // automatically, so in this example, consumption will only start from the
        // earliest message in the topic 'my-topic' the first time you run the program.
        AutoOffsetReset = AutoOffsetReset.Earliest
    };

    [ContextMenu("DebugPrint Kafka")]
    private void DebugStorageKafka() => Debug.LogWarning(storageKafka.ToString());
    public StorageKafka storageKafka = new StorageKafka()
    {
        topics=new List<string>(){"myTopic"}
    };

    private void OnDisable()
    {
        CancelConnect();
    }
        
    [ContextMenu("Connect Kafka")]
    public void TestConnect()=> _=Connect(_config, storageKafka);

    [ContextMenu("Disconnect Kafka")]
    public void CancelConnect() => _cts?.Cancel();
    
    // https://github.com/confluentinc/confluent-kafka-dotnet#basic-consumer-example
    private CancellationTokenSource _cts;
    async Task Connect(ConsumerConfig conf, StorageKafka storage)
    {
        Debug.LogFormat("KafkaConnect.Connect:{0} {1}", conf, storage);
        storage.Init();
        
        using (var c = new ConsumerBuilder<Ignore, string>(conf).Build())
        {
            foreach (var top in storage.topics)
            {
                c.Subscribe(top); //"myTopic"
                Debug.LogFormat("KafkaConnect.Connect Subscribe:{0}", top);
            }

            _cts = new CancellationTokenSource();

            try
            {
                while (true)
                {
                    try
                    {
                        var cr = await Task.Run(() => c.Consume(_cts.Token));//var cr = c.Consume(_cts.Token);
                        Debug.LogFormat("KafkaConnect.Connect Consumed message: {0} {1} {2} {3}", cr.Topic, cr.Offset, cr.Message.Timestamp.UtcDateTime, cr.Message.Value);
                        storage.Insert(cr.Topic, cr);
                    }
                    catch (ConsumeException e)
                    {
                        Debug.LogErrorFormat("KafkaConnect.Connect ConsumeException: {0}", e.Error.Reason);
                    }
                }
            }
            catch (OperationCanceledException e)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                c.Close();
                Debug.LogErrorFormat("KafkaConnect.Connect OperationCanceledException: {0}", e.Message);
            }
            catch(Exception e)
            {
                Debug.LogErrorFormat("KafkaConnect.Connect: {0}", e);
            }
        }
    }
}

[Serializable]
public class StorageKafka
{
    public List<string> topics;
    private Dictionary<string, Queue<ConsumeResult<Ignore, string>>> _storages;
    public void Insert(string topic, ConsumeResult<Ignore, string> message)
    {
        if (_storages == null || !_storages.ContainsKey(topic))
            return;
        
        _storages[topic].Enqueue(message);
    }

    public void Init()
    {
        if (topics == null || topics.Count == 0)
            return;
        
        _storages = new Dictionary<string, Queue<ConsumeResult<Ignore, string>>>();
        foreach (var topic in topics)
        {
            if (_storages.ContainsKey(topic))
                continue;
            _storages[topic] = new Queue<ConsumeResult<Ignore, string>>();
        }
    }
    
    private StringBuilder _sb = new StringBuilder();
    public override string ToString()
    {
        if (_storages == null)
            return "";

        _sb.Clear();
        foreach (var kvp in _storages)
        {
            _sb.AppendFormat("Key:{0} Cnt:{1}\n", kvp.Key, kvp.Value.Count);
            if (kvp.Value.Count == 0)
                continue;
            foreach(var val in kvp.Value.ToArray())
                _sb.AppendFormat("Val:{0} ", val.Message.Value);
        }

        return _sb.ToString();
    }
}
