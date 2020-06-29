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
