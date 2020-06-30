using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;
using System.Configuration;
using System.Data.SQLite;
using Newtonsoft.Json;
using System.Data;

namespace COVID_19_DATA_WS
{
    /// <summary>
    /// COVID_19_DATA_WS 的摘要说明
    /// </summary>
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // 若要允许使用 ASP.NET AJAX 从脚本中调用此 Web 服务，请取消注释以下行。 
    // [System.Web.Script.Services.ScriptService]
    public class COVID_19_DATA_WS : System.Web.Services.WebService
    {
        static string ConnStrSQLite = ConfigurationManager.ConnectionStrings["ConnStrSQLite"].ConnectionString;

        [WebMethod]
        public string HelloWorld()
        {
            return "Hello World";
        }


        [WebMethod(Description = "获取中国各省/自治区/直辖市/特别行政区时间序列数据")]
        public string GetChinaProvinceTimeSeriesData()
        {
            string sql =
                  @"SELECT t.provinceName,
                           t.provinceEnglishName,
                           t.province_confirmedCount,
                           t.province_curedCount,
                           t.province_deadCount,
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM China t
                     GROUP BY t.provinceName,
                              substr(t.updateTime, 0, 11) 
                     ORDER BY t.provinceEnglishName ASC";

            DataTable dt = ExecuteSQL(sql);
            return JsonConvert.SerializeObject(dt);
        }

        [WebMethod(Description = "获取中国各省级行政区截至某日数据")]
        // dataType 用于选择数据类型 0-全部数据 1-累计确诊 2-现存确诊 3-累计治愈 4-累计死亡
        public void GetChinaProvincetDateData(string date, int dataType)
        {
            string[] dataTypeArr =
                { "t.province_confirmedCount, t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount, t.province_curedCount, t.province_deadCount",
                  "t.province_confirmedCount as value",
                  "t.province_confirmedCount - t.province_curedCount - t.province_deadCount as value",
                  "t.province_curedCount as value",
                  "t.province_deadCount as value"};

            string sql =
                  @"SELECT s.standardName as name,
                           t.provinceEnglishName," + dataTypeArr[dataType] + @",
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM China t, Std_Name s
                     WHERE t.provinceName = s.provinceName AND date <= '" + date + @"'
                  GROUP BY t.provinceName
                  ORDER BY t.provinceEnglishName ASC";

            DataTable dt = ExecuteSQL(sql);

            dt.Columns.Remove("provinceEnglishName");
            dt.Columns.Remove("date");
            dt.Columns.Remove("updateTime");

            HttpContext.Current.Response.Write(JsonConvert.SerializeObject(dt));
        }

        [WebMethod(Description = "获取中国省级行政区截至某日的时间序列数据")]
        // dataType 用于选择数据类型 0-全部数据 1-累计确诊 2-现存确诊 3-累计治愈 4-累计死亡
        public void GetEachProvinceTimeSeriesData(string provinceName, string date, int dataType)
        {
            string[] dataTypeArr =
                { "t.province_confirmedCount, t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount, t.province_curedCount, t.province_deadCount",
                  "t.province_confirmedCount as value",
                  "t.province_confirmedCount - t.province_curedCount - t.province_deadCount as value",
                  "t.province_curedCount as value",
                  "t.province_deadCount as value"};

            string sql =
                  @"SELECT s.standardName as name,
                           t.provinceEnglishName," + dataTypeArr[dataType] + @",
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM China t, Std_Name s
                     WHERE t.provinceName = s.provinceName AND date <= '" + date + @"' AND s.standardName = '" + provinceName + @"'
                  GROUP BY date
                  ORDER BY date ASC";

            DataTable dt = ExecuteSQL(sql);

            dt.Columns.Remove("provinceEnglishName");
            dt.Columns.Remove("updateTime");

            List<long> valueY = dt.AsEnumerable().Select(d => d.Field<long>("value")).ToList();
            List<string> dateX = dt.AsEnumerable().Select(d => d.Field<string>("date")).ToList();
            string returnJson = "[" + JsonConvert.SerializeObject(dateX) + "," + JsonConvert.SerializeObject(valueY) + "]";

            HttpContext.Current.Response.Write(JsonConvert.SerializeObject(returnJson));
        }






        private static DataTable ExecuteSQL(string sql)
        {
            SQLiteConnection conn = new SQLiteConnection(ConnStrSQLite);
            SQLiteCommand cmd = new SQLiteCommand(sql, conn);

            try
            {
                conn.Open();
                SQLiteDataAdapter oda = new SQLiteDataAdapter(cmd);
                DataTable dt = new DataTable();
                oda.Fill(dt);

                return dt;
            }
            catch (Exception ex)
            {
                return null;
            }
            finally
            {
                conn.Close();
            }
        }
    }
}
