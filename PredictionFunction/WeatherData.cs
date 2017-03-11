using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


public class WeatherData
{
    public DateTime LastUpdate { get; set; }
    public DateTime NextUpdate { get; set; }
    public string LocationName { get; set; }
    public string LocationType { get; set; }
    public string LocationCountry { get; set; }
    public string LocationLatitude { get; set; }
    public string LocationLongitude { get; set; }

    public string SunRise { get; set; }
    public string SunSet { get; set; }

    public List<WeatherItem> WeatherItems { get; set; }

    public bool IsFilled { get; set; }

    public WeatherData()
    {
        WeatherItems = new List<WeatherItem>();
    }
}

