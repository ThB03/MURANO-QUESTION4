using System;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Net.Http;
using System.Net.WebSockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

public class BinanceOrderBookCollector
{
    private static ClientWebSocket webSocket;
    
    public static async Task Main()
    {
        await CollectOrderBook();
    }

    public static async Task CollectOrderBook()
    {
        // Open a WebSocket stream to receive real-time updates
        webSocket = new ClientWebSocket();
        await webSocket.ConnectAsync(new Uri("wss://stream.binance.com:9443/ws/bnbbtc@depth"), CancellationToken.None);

        // Buffer the events received from the stream
        List<JObject> eventBuffer = new List<JObject>();

        // Create a cancellation token source for WebSocket closure
        CancellationTokenSource cancellationTokenSource = new CancellationTokenSource();

        // Start receiving events in a separate thread
        Task receiveTask = ReceiveEvents(eventBuffer, cancellationTokenSource.Token);

        // Get the depth snapshot
        JObject snapshot = await GetDepthSnapshot();

        // Process the depth snapshot
        long lastUpdateId = (long)snapshot["lastUpdateId"];
        JArray bids = (JArray)snapshot["bids"];
        JArray asks = (JArray)snapshot["asks"];

        Dictionary<string, decimal> orderBook = ProcessSnapshot(bids, asks);

        // Drop any event where u is <= lastUpdateId in the snapshot
        eventBuffer.RemoveAll(eventData => (long)eventData["u"] <= lastUpdateId);

        // Check the first processed event's U and u
        bool isFirstEventProcessed = false;

        while (true)
        {
            if (eventBuffer.Count > 0)
            {
                // Get the oldest event from the buffer
                JObject eventData = eventBuffer[0];
                eventBuffer.RemoveAt(0);

                long U = (long)eventData["U"];
                long u = (long)eventData["u"];

                // Check the ordering of event IDs
                if (!isFirstEventProcessed)
                {
                    // The first processed event should have U <= lastUpdateId + 1 and u >= lastUpdateId + 1
                    if (U > lastUpdateId + 1 || u < lastUpdateId + 1)
                    {
                        Console.WriteLine("Invalid event ordering.");
                        break;
                    }
                    isFirstEventProcessed = true;
                }
                else
                {
                    // Each new event's U should be equal to the previous event's u + 1
                    if (U != lastUpdateId + 1)
                    {
                        Console.WriteLine("Invalid event ordering.");
                        break;
                    }
                }

                // Update the order book with the event data
                JArray bidsToUpdate = (JArray)eventData["b"];
                JArray asksToUpdate = (JArray)eventData["a"];

                // Process the bids to be updated
                orderBook = UpdateOrderBook(orderBook, bidsToUpdate, true);

                // Process the asks to be updated
                orderBook = UpdateOrderBook(orderBook, asksToUpdate, false);

                // Print the updated order book
                Console.WriteLine("Order Book:");
                foreach (var entry in orderBook)
                {
                    Console.WriteLine("Price: {0}, Quantity: {1}", entry.Key, entry.Value);
                }

                // Update the lastUpdateId with the latest event ID
                lastUpdateId = u;
            }

            // Wait for a short duration before processing the next event
            await Task.Delay(100);
        }

        // Close the WebSocket and cancel the receive task
        cancellationTokenSource.Cancel();
        await webSocket.CloseAsync(WebSocketCloseStatus.NormalClosure, "Closing WebSocket", CancellationToken.None);
        await receiveTask;
    }

    private static async Task ReceiveEvents(List<JObject> eventBuffer, CancellationToken cancellationToken)
    {
        byte[] receiveBuffer = new byte[1024];

        while (true)
        {
            if (webSocket.State != WebSocketState.Open)
                break;

            WebSocketReceiveResult result = await webSocket.ReceiveAsync(new ArraySegment<byte>(receiveBuffer), cancellationToken);

            if (result.MessageType == WebSocketMessageType.Text)
            {
                string eventData = Encoding.UTF8.GetString(receiveBuffer, 0, result.Count);
                JObject eventJson = JObject.Parse(eventData);
                eventBuffer.Add(eventJson);
            }
        }
    }

    private static async Task<JObject> GetDepthSnapshot()
    {
        using (var client = new HttpClient())
        {
            string snapshotJson = await client.GetStringAsync("https://api.binance.com/api/v3/depth?symbol=BNBBTC&limit=1000");
            JObject snapshot = JObject.Parse(snapshotJson);
            return snapshot;
        }
    }

    private static Dictionary<string, decimal> ProcessSnapshot(JArray bids, JArray asks)
    {
        Dictionary<string, decimal> orderBook = new Dictionary<string, decimal>();

        foreach (JArray bid in bids)
        {
            string price = (string)bid[0];
            decimal quantity = (decimal)bid[1];
            orderBook[price] = quantity;
        }

        foreach (JArray ask in asks)
        {
            string price = (string)ask[0];
            decimal quantity = (decimal)ask[1];
            orderBook[price] = quantity;
        }

        return orderBook;
    }

    private static Dictionary<string, decimal> UpdateOrderBook(Dictionary<string, decimal> orderBook, JArray updateData, bool isBid)
    {
        foreach (JArray entry in updateData)
        {
            string price = (string)entry[0];
            decimal quantity = (decimal)entry[1];

            // Update the absolute quantity for a price level
            if (quantity > 0)
                orderBook[price] = quantity;
            else
                orderBook.Remove(price); // Remove the price level if the quantity is zero
        }

        return orderBook;
    }
}
