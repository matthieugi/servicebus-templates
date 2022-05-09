using System;
using System.Threading.Tasks;
using System.Threading;
using Receive;

namespace Receive
{
    class Program
    {
        public static async Task<int> Main() {
            ReceiveWorker receive = new ReceiveWorker();

            List<Task> processors = new List<Task>();

            for(int i = 0; i < 32; i++){
                for(int j=0; j < 8; j++){
                    processors.Add(receive.Run($"dauphine-publi-{i}", $"publi-{j}"));
                }
            }
            Task.WaitAll(processors.ToArray());

            await ReceiveWorker.ShowStats();

            return 0;
        }
    }
}