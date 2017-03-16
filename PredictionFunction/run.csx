#load "ForecastDBItem.cs"
#load "PredictionDBItem.cs"
#load "WeatherData.cs"
#load "WeatherItem.cs"
#load "MLBatchRow.cs"

#r "Microsoft.WindowsAzure.Storage"

using FastMember;
using RestSharp;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml;
using System.Diagnostics;
using System.Net.Http;
using System.Net.Http.Formatting;
using System.Net.Http.Headers;
using System.Runtime.Serialization;
using System.Threading;
using System.Configuration;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;


public static void Run(TimerInfo myTimer, TraceWriter log)
{
    var data = GetWeatherData();
    SaveForecastToDB(data);
    //Console.WriteLine(GetWeatherXml().ToString());

    CreateMLBatchFile(data);
    //InvokeBatchExecutionService().Wait();

    //Reschedule function based on next update
    RescheduleTimeTrigger(data, log);

    log.Info("Finished" + DateTime.Now.ToString());
}

/// <summary>
/// Create a new schedule
/// </summary>
/// <param name="data"></param>
private static void RescheduleTimeTrigger(WeatherData data, TraceWriter log)
{
    string[] settings = File.ReadAllLines(@"D:\home\site\wwwroot\PredictionFunction\function.json");
    var schedule = settings[7];
    log.Info($"Current schedule: {schedule}");

    var now = DateTime.Now;

    //Check whether NextUpdate is not corrupted
    if (data.NextUpdate <= now)
    {
        now = now.AddHours(1);
        schedule = $"\t\t\"schedule\": \"0 {now.Minute} {now.Hour} {now.Day} * *\"";
    }
    else

        schedule = $"\t\t\"schedule\": \"0 {data.NextUpdate.Minute} {data.NextUpdate.Hour} {data.NextUpdate.Day} * *\"";
    //Overwrite original settings file
    settings[7] = schedule;
    File.WriteAllLines(@"D:\home\site\wwwroot\PredictionFunction\function.json", settings);
    log.Info($"New schedule: {settings}");
}

static string url = "http://www.yr.no/place/Czech_Republic/Prague/Prague/forecast_hour_by_hour.xml";

static string dbConnectionString = ConfigurationManager.ConnectionStrings["sqldb_connection"].ConnectionString;

static string mlWeatherAssociation = "0,0,0,1,2,8,9,7,7,6,6,9,7,7,9,4,0,0,0,0,9,9,9,9,9,9,9,9,9,9,6,7,3,7,9,0,0,0,0,0,8,8,7,7,0,7,5,7,7,7,7";
public static string GetWeatherXml()
{
    var client = new RestClient(url);
    var request = new RestRequest(Method.GET);
    return client.Execute(request).Content;
}

public static WeatherData GetWeatherData()
{
    WeatherData Data = new WeatherData() { WeatherItems = new List<WeatherItem>() };

    StringBuilder sb = new StringBuilder("");

    System.Xml.XmlDocument doc = new XmlDocument();

    doc.LoadXml(GetWeatherXml());

    System.Xml.XmlNode sun = doc.SelectSingleNode("/weatherdata/sun");
    System.Xml.XmlNode location = doc.SelectSingleNode("/weatherdata/location");
    System.Xml.XmlNodeList nodelist = doc.SelectNodes("/weatherdata/forecast/tabular/time");

    System.Xml.XmlNode meta = doc.SelectSingleNode("/weatherdata/meta");
    var lastupdatenode = meta.SelectSingleNode("lastupdate");
    Data.LastUpdate = Convert.ToDateTime(lastupdatenode.InnerText);
    Data.NextUpdate = Convert.ToDateTime(meta["nextupdate"].InnerText);

    Data.SunRise = sun.Attributes["rise"].Value;
    Data.SunSet = sun.Attributes["set"].Value;
    Data.IsFilled = true;

    Data.LocationName = location["name"].InnerText;
    Data.LocationType = location["type"].InnerText;
    Data.LocationCountry = location["country"].InnerText;

    Data.LocationLatitude = location["location"].Attributes["latitude"].Value;
    Data.LocationLongitude = location["location"].Attributes["longitude"].Value;

    for (int i = 0; i < 7; i++)
    {
        WeatherItem item = new WeatherItem();

        // item.Period = nodelist[i].Attributes["period"].Value;
        item.TimeFrom = DateTime.Parse(nodelist[i].Attributes["from"].Value);
        item.TimeTo = DateTime.Parse(nodelist[i].Attributes["to"].Value);
        item.SymbolNumber = nodelist[i]["symbol"].Attributes["number"].Value.PadLeft(2, '0');
        item.SymbolName = nodelist[i]["symbol"].Attributes["name"].Value;
        item.Temp = nodelist[i]["temperature"].Attributes["value"].Value;
        item.PrecipitationValue = nodelist[i]["precipitation"].Attributes["value"].Value;
        item.WindSpeedMPS = nodelist[i]["windSpeed"].Attributes["mps"].Value;
        item.WindSpeedName = nodelist[i]["windSpeed"].Attributes["name"].Value;
        item.WindDirectionCode = nodelist[i]["windDirection"].Attributes["code"].Value;
        item.WindDirectionDeg = nodelist[i]["windDirection"].Attributes["deg"].Value;
        item.WindDirectionName = nodelist[i]["windDirection"].Attributes["name"].Value;
        item.Pressure = nodelist[i]["pressure"].Attributes["value"].Value;

        item.IsFilled = true;

        Data.WeatherItems.Add(item);
    }

    return Data;
}

public static void SaveForecastToDB(WeatherData wd)
{
    List<ForecastDBItem> forecasts = new List<ForecastDBItem>();

    //We need forecasts for next 48 hours
    foreach (var wi in wd.WeatherItems.Where(tmpwi => tmpwi.TimeFrom < DateTime.Now.AddHours(48)))
    {
        forecasts.Add(new ForecastDBItem() { TimeFrom = wi.TimeFrom, TimeTo = wi.TimeTo, LastUpdated = wd.LastUpdate, Precipitation = Convert.ToDouble(wi.PrecipitationValue), Pressure = Convert.ToDouble(wi.Pressure, new CultureInfo("en-US")), WindSpeedMPS = Convert.ToDouble(wi.WindSpeedMPS, new CultureInfo("en-US")), Temp = Convert.ToDouble(wi.Temp), SymbolNumber = Convert.ToInt32(wi.SymbolNumber) });
    }

    //var str = ConfigurationManager.ConnectionStrings["sqldb_connection"].ConnectionString;

    using (var bcp = new SqlBulkCopy(dbConnectionString))
    using (var reader = ObjectReader.Create(forecasts))
    { // note that you can be more selective with the column map
        bcp.DestinationTableName = "dbo.Forecast";
        bcp.ColumnMappings.Add("TimeFrom", "TimeFrom");
        bcp.ColumnMappings.Add("TimeTo", "TimeTo");
        bcp.ColumnMappings.Add("LastUpdated", "LastUpdated");
        bcp.ColumnMappings.Add("Precipitation", "Precipitation");
        bcp.ColumnMappings.Add("Pressure", "Pressure");
        bcp.ColumnMappings.Add("Temp", "Temp");
        bcp.ColumnMappings.Add("WindSpeedMPS", "WindSpeedMPS");
        bcp.ColumnMappings.Add("SymbolNumber", "SymbolNumber");
        bcp.WriteToServer(reader);
    }
}

private static void CreateMLBatchFile(WeatherData data)
{
    var conn = new SqlConnection(dbConnectionString);
    conn.Open();

    DateTime now = DateTime.Now;
    var changeTime = now;
    var today = new DateTime(now.Year, now.Month, now.Day, 0, 0, 0);

    var firstDay = DateTime.MinValue;
    MLBatchRow row = new MLBatchRow();


    string startDate = "";
    SqlCommand sqlcmd = new SqlCommand($"Select TOP (1) * from dbo.tabulka WHERE created<'{now.AddDays(-7).AddHours(-48).AddMinutes(20).ToString("yyyy-MM-dd HH:mm:ss.fff")}' ORDER BY created DESC;", conn);
    using (SqlDataReader reader = sqlcmd.ExecuteReader())
    {
        if (reader.Read())
        {
            startDate = Convert.ToDateTime(reader["created"]).ToString("yyyy-MM-dd HH:mm:ss.fff");
        }
    }

    string endDate = "";
    sqlcmd = new SqlCommand($"Select TOP(1) * from dbo.tabulka WHERE created>'{now.AddDays(-7).ToString("yyyy - MM - dd HH: mm:ss.fff")}' ORDER BY created ASC;", conn);
    using (SqlDataReader reader = sqlcmd.ExecuteReader())
    {
        if (reader.Read())
        {
            endDate = Convert.ToDateTime(reader["created"]).ToString("yyyy-MM-dd HH:mm:ss.fff");
        }
    }

    List<Tuple<string, int>> HistoryData = new List<Tuple<string, int>>();

    sqlcmd = new SqlCommand($"Select * from dbo.tabulka WHERE created >= '{startDate}' AND created <= '{endDate}' ORDER BY created DESC", conn);
    using (SqlDataReader reader = sqlcmd.ExecuteReader())
    {
        while (reader.Read() != false)
        {
            HistoryData.Add(new Tuple<string, int>(reader["created"].ToString(), Convert.ToInt32(reader["free"])));
        }
    }

    string dateString;
    using (System.IO.StreamWriter file = new System.IO.StreamWriter(@"D:\home\site\wwwroot\PredictionFunction\inputdata.csv"))
    {

        file.WriteLine("datetime,WeekDay,BDay,Weekend,Holiday,ProbVacation,Feasts,Hour,Minute,WIND_SPD,TEMP_TEMP,PSWX_JOINT,PRECIP_AMT,v_free_h0m,v_free_h5m,v_free_h10m,v_free_h15m");

        while (changeTime <= now.AddMinutes(15)) //.AddHours(48))
        {
            if (firstDay.Day < changeTime.Day)
            {
                firstDay = changeTime;
                dateString = changeTime.ToString("yyyy-MM-dd 00:00:00.000");
                SqlCommand command = new SqlCommand($"Select * from dbo.calendar WHERE Date='{dateString}';", conn);
                // int result = command.ExecuteNonQuery();
                using (SqlDataReader reader = command.ExecuteReader())
                {
                    if (reader.Read())
                    {
                        row.BDay = Convert.ToBoolean(reader["Bday"]);
                        row.Feasts = Convert.ToBoolean(reader["Feasts"]);
                        row.Holiday = Convert.ToBoolean(reader["Holiday"]);
                        row.ProbVacation = Convert.ToBoolean(reader["ProbVacation"]);
                        row.WeekDay = reader["WeekDay"].ToString();
                        row.Weekend = Convert.ToBoolean(reader["Weekend"]);
                    }
                }
            }

            //Get history data

            dateString = changeTime.AddDays(-7).ToString("yyyy-MM-dd HH:mm:ss.fff");
            row.v_free_h = HistoryData.Where(hd => Convert.ToDateTime(hd.Item1) <= Convert.ToDateTime(dateString)).FirstOrDefault().Item2;

            dateString = changeTime.AddDays(-7).AddMinutes(-5).ToString("yyyy-MM-dd HH:mm:ss.fff");
            row.v_free_h5m = HistoryData.Where(hd => Convert.ToDateTime(hd.Item1) <= Convert.ToDateTime(dateString)).FirstOrDefault().Item2;


            dateString = changeTime.AddDays(-7).AddMinutes(-10).ToString("yyyy-MM-dd HH:mm:ss.fff");
            row.v_free_h10m = HistoryData.Where(hd => Convert.ToDateTime(hd.Item1) <= Convert.ToDateTime(dateString)).FirstOrDefault().Item2;

            dateString = changeTime.AddDays(-7).AddMinutes(-15).ToString("yyyy-MM-dd HH:mm:ss.fff");
            row.v_free_h15m = HistoryData.Where(hd => Convert.ToDateTime(hd.Item1) <= Convert.ToDateTime(dateString)).FirstOrDefault().Item2;

            var wi = data.WeatherItems.Where(item => item.TimeFrom <= changeTime && item.TimeTo >= changeTime).FirstOrDefault();
            if (wi != null)
            {
                file.WriteLine($"{changeTime.ToString("yyyy-MM-dd HH:mm:ss:fff")},{row.WeekDay},{row.BDay}, {row.Weekend},{row.Holiday},{row.ProbVacation},{row.Feasts},{changeTime.Hour},{changeTime.Minute},{wi.WindSpeedMPS},{wi.Temp},{mlWeatherAssociation[Convert.ToInt32(wi.SymbolNumber)]},{wi.PrecipitationValue},{row.v_free_h},{row.v_free_h5m}, {row.v_free_h10m}, {row.v_free_h15m}");

            }
            //datetime,WeekDay,BDay,Weekend,Holiday,ProbVacation,Feasts,Hour,Minute,WIND_SPD,TEMP_TEMP,PSWX_JOINT,PRECIP_AMT,v_free_h0m,v_free_h5m,v_free_h10m,v_free_h15m
            //,1,false,false,false,false,false,1,1,1,1,1,1,1,1,1,1

            changeTime = changeTime.AddMinutes(5);
        }
    }

    conn.Close();
}

public class AzureBlobDataReference
{
    // Storage connection string used for regular blobs. It has the following format:
    // DefaultEndpointsProtocol=https;AccountName=ACCOUNT_NAME;AccountKey=ACCOUNT_KEY
    // It's not used for shared access signature blobs.
    public string ConnectionString { get; set; }

    // Relative uri for the blob, used for regular blobs as well as shared access
    // signature blobs.
    public string RelativeLocation { get; set; }

    // Base url, only used for shared access signature blobs.
    public string BaseLocation { get; set; }

    // Shared access signature, only used for shared access signature blobs.
    public string SasBlobToken { get; set; }
}

public enum BatchScoreStatusCode
{
    NotStarted,
    Running,
    Failed,
    Cancelled,
    Finished
}

public class BatchScoreStatus
{
    // Status code for the batch scoring job
    public BatchScoreStatusCode StatusCode { get; set; }

    // Locations for the potential multiple batch scoring outputs
    public IDictionary<string, AzureBlobDataReference> Results { get; set; }

    // Error details, if any
    public string Details { get; set; }
}

public class BatchExecutionRequest
{

    public IDictionary<string, AzureBlobDataReference> Inputs { get; set; }

    public IDictionary<string, string> GlobalParameters { get; set; }

    // Locations for the potential multiple batch scoring outputs
    public IDictionary<string, AzureBlobDataReference> Outputs { get; set; }
}

static async Task WriteFailedResponse(HttpResponseMessage response)
{
    Console.WriteLine(string.Format("The request failed with status code: {0}", response.StatusCode));

    // Print the headers - they include the requert ID and the timestamp, which are useful for debugging the failure
    Console.WriteLine(response.Headers.ToString());

    string responseContent = await response.Content.ReadAsStringAsync();
    Console.WriteLine(responseContent);
}

static void SaveMLResultToDBFile(AzureBlobDataReference blobLocation, string resultsLabel)
{
    //const string OutputFileLocation = "myresultsfile.csv"; 

    var credentials = new StorageCredentials(blobLocation.SasBlobToken);
    var blobUrl = new Uri(new Uri(blobLocation.BaseLocation), blobLocation.RelativeLocation);
    var cloudBlob = new CloudBlockBlob(blobUrl, credentials);

    Console.WriteLine(string.Format("Reading the result from {0}", blobUrl.ToString()));
    List<PredictionDBItem> predictions = new List<PredictionDBItem>();
    string line;
    using (var resultFileStreamReader = new StreamReader(cloudBlob.OpenRead()))
    {
        //Read out headers
        resultFileStreamReader.ReadLine();

        while ((line = resultFileStreamReader.ReadLine()) != null)
        {
            var resLine = line.Split(',');
            predictions.Add(new PredictionDBItem() { Free = Convert.ToInt32(resLine[0]), Date = Convert.ToDateTime(resLine[1]) });
        }
    }
    using (var bcp = new SqlBulkCopy(dbConnectionString))
    using (var reader = ObjectReader.Create(predictions))
    { // note that you can be more selective with the column map
        bcp.DestinationTableName = "dbo.Predictions";
        bcp.ColumnMappings.Add("Free", "Free");
        bcp.ColumnMappings.Add("Date", "Date");

        bcp.WriteToServer(reader);
    }

    //cloudBlob.DownloadToFile(OutputFileLocation, FileMode.Create);
    //Console.WriteLine(string.Format("{0} have been written to the file {1}", resultsLabel, OutputFileLocation));
}

static void UploadFileToBlob(string inputFileLocation, string inputBlobName, string storageContainerName, string storageConnectionString)
{
    // Make sure the file exists
    if (!File.Exists(inputFileLocation))
    {
        throw new FileNotFoundException(
            string.Format(
                CultureInfo.InvariantCulture,
                "File {0} doesn't exist on local computer.",
                inputFileLocation));
    }

    Console.WriteLine("Uploading the input to blob storage...");

    var blobClient = CloudStorageAccount.Parse(storageConnectionString).CreateCloudBlobClient();
    var container = blobClient.GetContainerReference(storageContainerName);
    container.CreateIfNotExists();
    var blob = container.GetBlockBlobReference(inputBlobName);
    blob.UploadFromFile(inputFileLocation, FileMode.Open);
}

static void ProcessResults(BatchScoreStatus status)
{
    bool first = true;
    foreach (var output in status.Results)
    {
        var blobLocation = output.Value;
        Console.WriteLine(string.Format("The result '{0}' is available at the following Azure Storage location:", output.Key));
        Console.WriteLine(string.Format("BaseLocation: {0}", blobLocation.BaseLocation));
        Console.WriteLine(string.Format("RelativeLocation: {0}", blobLocation.RelativeLocation));
        Console.WriteLine(string.Format("SasBlobToken: {0}", blobLocation.SasBlobToken));
        Console.WriteLine();

        // Save the first output to disk
        if (first)
        {
            first = false;
            SaveMLResultToDBFile(blobLocation, string.Format("The results for {0}", output.Key));
        }
    }
}

static async Task InvokeBatchExecutionService()
{
    // How this works:
    //
    // 1. Assume the input is present in a local file (if the web service accepts input)
    // 2. Upload the file to an Azure blob - you'd need an Azure storage account
    // 3. Call the Batch Execution Service to process the data in the blob. Any output is written to Azure blobs.
    // 4. Download the output blob, if any, to local file

    string BaseUrl = ConfigurationManager.AppSettings["mlEndpoint"].ToString();

    string StorageAccountName = ConfigurationManager.AppSettings["storageName"].ToString();
    string StorageAccountKey = ConfigurationManager.AppSettings["storageKey"].ToString(); 
    string StorageContainerName = "mycontainer"; 
    string storageConnectionString = string.Format("DefaultEndpointsProtocol=https;AccountName={0};AccountKey={1}", StorageAccountName, StorageAccountKey);

    string apiKey = ConfigurationManager.AppSettings["mlApiKey"].ToString(); 

    // set a time out for polling status
    const int TimeOutInMilliseconds = 120 * 1000; // Set a timeout of 2 minutes

    UploadFileToBlob(@"D:\home\site\wwwroot\PredictionFunction\inputdata.csv" /*Replace this with the location of your input file, and valid file extension (usually .csv)*/,
            "input1datablob.csv" /*Replace this with the name you would like to use for your Azure blob; this needs to have the same extension as the input file */,
            StorageContainerName, storageConnectionString);

    using (HttpClient client = new HttpClient())
    {
        var request = new BatchExecutionRequest()
        {
            Inputs = new Dictionary<string, AzureBlobDataReference>() {
                        {
                            "input1",
                             new AzureBlobDataReference()
                             {
                                 ConnectionString = storageConnectionString,
                                 RelativeLocation = string.Format("{0}/input1datablob.csv", StorageContainerName)
                             }
                        },
                    },

            Outputs = new Dictionary<string, AzureBlobDataReference>() {
                        {
                            "output1",
                            new AzureBlobDataReference()
                            {
                                ConnectionString = storageConnectionString,
                                RelativeLocation = string.Format("{0}/output1results.csv", StorageContainerName)
                            }
                        },
                    },

            GlobalParameters = new Dictionary<string, string>()
            {
            }
        };

        client.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Bearer", apiKey);

        // WARNING: The 'await' statement below can result in a deadlock
        // if you are calling this code from the UI thread of an ASP.Net application.
        // One way to address this would be to call ConfigureAwait(false)
        // so that the execution does not attempt to resume on the original context.
        // For instance, replace code such as:
        //      result = await DoSomeTask()
        // with the following:
        //      result = await DoSomeTask().ConfigureAwait(false)

        Console.WriteLine("Submitting the job...");

        // submit the job
        var response = await client.PostAsJsonAsync(BaseUrl + "?api-version=2.0", request);

        if (!response.IsSuccessStatusCode)
        {
            await WriteFailedResponse(response);
            return;
        }

        string jobId = await response.Content.ReadAsAsync<string>();
        Console.WriteLine(string.Format("Job ID: {0}", jobId));

        // start the job
        Console.WriteLine("Starting the job...");
        response = await client.PostAsync(BaseUrl + "/" + jobId + "/start?api-version=2.0", null);
        if (!response.IsSuccessStatusCode)
        {
            await WriteFailedResponse(response);
            return;
        }

        string jobLocation = BaseUrl + "/" + jobId + "?api-version=2.0";
        Stopwatch watch = Stopwatch.StartNew();
        bool done = false;
        while (!done)
        {
            Console.WriteLine("Checking the job status...");
            response = await client.GetAsync(jobLocation);
            if (!response.IsSuccessStatusCode)
            {
                await WriteFailedResponse(response);
                return;
            }

            BatchScoreStatus status = await response.Content.ReadAsAsync<BatchScoreStatus>();
            if (watch.ElapsedMilliseconds > TimeOutInMilliseconds)
            {
                done = true;
                Console.WriteLine(string.Format("Timed out. Deleting job {0} ...", jobId));
                await client.DeleteAsync(jobLocation);
            }
            switch (status.StatusCode)
            {
                case BatchScoreStatusCode.NotStarted:
                    Console.WriteLine(string.Format("Job {0} not yet started...", jobId));
                    break;
                case BatchScoreStatusCode.Running:
                    Console.WriteLine(string.Format("Job {0} running...", jobId));
                    break;
                case BatchScoreStatusCode.Failed:
                    Console.WriteLine(string.Format("Job {0} failed!", jobId));
                    Console.WriteLine(string.Format("Error details: {0}", status.Details));
                    done = true;
                    break;
                case BatchScoreStatusCode.Cancelled:
                    Console.WriteLine(string.Format("Job {0} cancelled!", jobId));
                    done = true;
                    break;
                case BatchScoreStatusCode.Finished:
                    done = true;
                    Console.WriteLine(string.Format("Job {0} finished!", jobId));
                    ProcessResults(status);
                    break;
            }

            if (!done)
            {
                Thread.Sleep(1000); // Wait one second
            }
        }
    }
}
