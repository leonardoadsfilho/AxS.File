using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Collections.Generic;

string filePath = "example.txt"; // Caminho do arquivo de origem
string outputFilePath = "output.txt"; // Caminho do arquivo de saída
int numLinesPerTask = 1000; // Número de linhas por tarefa
int maxAmountTasks = 10;

try
{
    using var inputFileStream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan);
    using var outputFileStream = new FileStream(outputFilePath, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.SequentialScan);
    using var reader = new StreamReader(inputFileStream);
    using var writer = new StreamWriter(outputFileStream);

    // Lê o tamanho da primeira linha
    int lineSize = (await reader.ReadLineAsync())?.Length + 2 ?? 0;
    if (lineSize == 0)
    {
        Console.WriteLine("Error: Empty file.");
        return;
    }

    // Calcula o número de tarefas necessárias
    long fileSize = inputFileStream.Length;
    int numTasks = (int)Math.Ceiling((double)fileSize / (numLinesPerTask * lineSize));

    if(numTasks >= maxAmountTasks){
        numTasks = maxAmountTasks;
        numLinesPerTask = (int)Math.Ceiling((double)fileSize / lineSize / numTasks);
    }
    
    // Calcula o tamanho do buffer baseado no número de linhas
    int bufferSize = numLinesPerTask * lineSize;

    // Cria um semáforo para controlar o acesso ao arquivo de saída
    SemaphoreSlim semaphore = new SemaphoreSlim(1, 1);

    // Cria e executa as tarefas
    await CreateAndRunTasks(inputFileStream, numTasks, numLinesPerTask, lineSize, bufferSize, writer, semaphore);
}
catch (FileNotFoundException)
{
    Console.WriteLine("File not found.");
}
catch (Exception ex)
{
    Console.WriteLine($"Error: {ex.Message}");
}

static async Task CreateAndRunTasks(FileStream fileStream, int numTasks, int numLinesPerTask, int lineSize, int bufferSize, StreamWriter writer, SemaphoreSlim semaphore)
{
    // Cria uma lista para armazenar as tarefas
    List<Task> tasks = new List<Task>();

    for (int i = 0; i < numTasks; i++)
    {
        long startPosition = i * numLinesPerTask * lineSize;
        await Task.Delay(TimeSpan.FromMilliseconds(100));
        tasks.Add(Task.Run(() => ReadAndWriteFileChunk(fileStream, startPosition, numLinesPerTask, lineSize, bufferSize, writer, semaphore)));
    }

    // Aguarda a conclusão de todas as tarefas
    await Task.WhenAll(tasks);
}

static async Task ReadAndWriteFileChunk(FileStream fileStream, long startPosition, int numLinesToRead, int lineSize, int bufferSize, StreamWriter writer, SemaphoreSlim semaphore)
{
    StringBuilder stringBuilder = new StringBuilder(); // StringBuilder para armazenar o conteúdo lido pela tarefa

    try{
        byte[] buffer = new byte[bufferSize]; // Tamanho do buffer calculado dinamicamente
        
        // Posiciona o cursor no início da seção da tarefa
        fileStream.Seek(startPosition, SeekOrigin.Begin);

        // Realiza a leitura do buffer
        int bytesRead = await fileStream.ReadAsync(buffer, 0, buffer.Length);

        // Verifica se houve bytes lidos
        if (bytesRead > 0){
            // Converte os bytes lidos em linhas de texto e as adiciona ao StringBuilder
            for (int i = 0; i < bytesRead; i += lineSize + 2){ // +2 para incluir \r\n
                // Calcular o comprimento da linha, incluindo \r\n
                int lineLength = Math.Min(lineSize + 2, bytesRead - i); // Se for menor que o tamanho da linha, usar bytesRead - i
                string line = Encoding.UTF8.GetString(buffer, i, lineLength);
                stringBuilder.Append(line);
            }

            // Aguarda o acesso exclusivo ao arquivo de saída
            await semaphore.WaitAsync();
            // Escreve o conteúdo lido pelo StringBuilder da tarefa no arquivo de saída
            await writer.WriteAsync(stringBuilder.ToString());

            // Libera o semáforo
            semaphore.Release();
        }
    }
    catch (Exception ex){
        // Em caso de erro, mostra a mensagem e retorna
        Console.WriteLine($"Error reading or writing file: {ex.Message}");
        return;
    }
}
