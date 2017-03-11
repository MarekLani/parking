using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


public class WeatherItem
{
    public DateTime TimeFrom { get; set; }
    public DateTime TimeTo { get; set; }

    //public string Period { get; set; }

    public string SymbolNumber { get; set; }
    public string SymbolName { get; set; }
    public string SymbolVar { get; set; }

    public string PrecipitationValue { get; set; }

    public string WindDirectionDeg { get; set; }
    public string WindDirectionCode { get; set; }
    public string WindDirectionName { get; set; }
    public string WindSpeedMPS { get; set; }
    public string WindSpeedName { get; set; }

    public string Temp { get; set; }
    public string Pressure { get; set; }

    public bool IsFilled { get; set; }

}
