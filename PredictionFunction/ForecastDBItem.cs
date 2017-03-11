using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


class ForecastDBItem
{
    public DateTime TimeFrom { get; set; }
    public DateTime TimeTo { get; set; }

    public DateTime LastUpdated { get; set; }


    public int SymbolNumber { get; set; }

    public double Precipitation { get; set; }

    public double WindSpeedMPS { get; set; }
 
    public double Temp { get; set; }
    public double Pressure { get; set; }

}

