using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

    public class MLBatchRow
    {
        public string WeekDay { get; set; }
        public bool BDay { get; set; }
        public bool Weekend { get; set; }
        public bool Holiday { get; set; }
        public bool ProbVacation { get; set; }
        public bool Feasts { get; set; }

        public string WIND_SPD { get; set; }
        public string TEMP_TEMP { get; set; }
        public string PSWX_JOINT { get; set; }
        public string PRECIP_AMT { get; set; }

        public int v_free_h { get; set; }
        public int v_free_h5m {get; set;}
        public int v_free_h10m { get; set; }
        public int v_free_h15m { get; set; }

//        datetime,WeekDay,BDay,Weekend,Holiday,ProbVacation,Feasts,Hour,Minute,WIND_SPD,TEMP_TEMP,PSWX_JOINT,PRECIP_AMT,v_free_h0m,v_free_h5m,v_free_h10m,v_free_h15m
//,1,false,false,false,false,false,1,1,1,1,1,1,1,1,1,1


    }
