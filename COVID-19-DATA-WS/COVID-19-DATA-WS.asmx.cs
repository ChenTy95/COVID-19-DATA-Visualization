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

        static readonly string[] dataTypeStr = { "", "累计确诊", "现存确诊", "累计治愈", "累计死亡" };

        [WebMethod(Description = "获取中国各省/自治区/直辖市/特别行政区截至某日的时间序列数据")]
        // dataType 用于选择数据类型 1-累计确诊 2-现存确诊 3-累计治愈 4-累计死亡
        public void GetChinaProvinceTimeSeriesData(string date, int dataType)
        {
            string sql =
                  @"SELECT s.standardName as name,
                           t.provinceEnglishName,
                           t.province_confirmedCount,
                           t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount,
                           t.province_curedCount,
                           t.province_deadCount,
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM China t, Std_Name s
                     WHERE t.provinceName = s.provinceName AND date <= '" + date + @"'
                     GROUP BY t.provinceName, substr(t.updateTime, 0, 11) 
                     ORDER BY name ASC, date ASC";

            DataTable dt = ExecuteSQL(sql);

            dt.Columns.Remove("provinceEnglishName");
            dt.Columns.Remove("updateTime");

            // 日期数据插值
            int i = 0;
            DateTime dt0 = Convert.ToDateTime("2020-01-22");
            DateTime dtn = Convert.ToDateTime(date);
            // 此时 i 指向 dt 第一行
            while (dt.Rows[0]["date"].ToString() != dt0.ToString("yyyy-MM-dd"))
            {
                DateTime dti0 = Convert.ToDateTime(dt.Rows[i]["date"].ToString()).AddDays(-1);
                DataRow dr = dt.NewRow();
                for (int t = 0; t < dt.Columns.Count; t++)
                {
                    if (t >= 1 && t <= 4)
                        dr[t] = 0;
                    else
                        dr[t] = dt.Rows[i].ItemArray[t];
                }
                dt.Rows.InsertAt(dr, 0);
                dt.Rows[0]["date"] = dti0.ToString("yyyy-MM-dd");
            }
            while (i < dt.Rows.Count - 1)
            {
                DateTime dti = Convert.ToDateTime(dt.Rows[i]["date"].ToString());
                DateTime dti0 = Convert.ToDateTime(dt.Rows[i + 1]["date"].ToString()).AddDays(-1);
                DateTime dti1 = Convert.ToDateTime(dt.Rows[i]["date"].ToString()).AddDays(1);
                DateTime dtj = Convert.ToDateTime(dt.Rows[i + 1]["date"].ToString());

                bool headFlag = false;
                if (dt.Rows[i]["name"].ToString() != dt.Rows[i + 1]["name"].ToString() && dtj != dt0)
                {
                    DataRow dr = dt.NewRow();
                    for (int t = 0; t < dt.Columns.Count; t++)
                    {
                        if (t >= 1 && t <= 4)
                            dr[t] = 0;
                        else
                            dr[t] = dt.Rows[i + 1].ItemArray[t];
                    }
                    dt.Rows.InsertAt(dr, i + 1);
                    dt.Rows[i + 1]["date"] = dti0.ToString("yyyy-MM-dd");
                    headFlag = true;
                }

                bool middleFlag = false;
                if ((dt.Rows[i]["name"].ToString() == dt.Rows[i + 1]["name"].ToString() && dtj != dti1) ||
                    (dt.Rows[i]["name"].ToString() != dt.Rows[i + 1]["name"].ToString() && dti != dtn))
                {
                    DataRow dr = dt.NewRow();
                    for (int t = 0; t < dt.Columns.Count; t++)
                        dr[t] = dt.Rows[i].ItemArray[t];
                    dt.Rows.InsertAt(dr, i + 1);
                    dt.Rows[i + 1]["date"] = dti1.ToString("yyyy-MM-dd");
                    middleFlag = true;
                }

                if (!(headFlag == true && middleFlag == false))
                    i++;
            }
            // 此时 i 指向 dt 最后一行
            while (dt.Rows[i]["date"].ToString() != dtn.ToString("yyyy-MM-dd"))
            {
                DateTime dti1 = Convert.ToDateTime(dt.Rows[i]["date"].ToString()).AddDays(1);
                DataRow dr = dt.NewRow();
                for (int t = 0; t < dt.Columns.Count; t++)
                    dr[t] = dt.Rows[i].ItemArray[t];
                dt.Rows.InsertAt(dr, i + 1);
                dt.Rows[i + 1]["date"] = dti1.ToString("yyyy-MM-dd");
                i++;
            }

            int totalDays = (int)(Convert.ToDateTime(date) - Convert.ToDateTime("2020-01-22")).TotalDays;
            string[] dataStrArr = new string[totalDays + 1];
            DataTable dtNew = new DataTable();
            dtNew.Columns.Add("name", typeof(string));
            dtNew.Columns.Add("value", typeof(int));

            for (int t = 0; t <= totalDays; t++)
            {
                string dtDay = Convert.ToDateTime("2020-01-22").AddDays(t).ToString("yyyy-MM-dd");
                DataRow[] drArr = dt.Select("date = '" + dtDay + "'");
                for (int j = 0; j < drArr.Length; j++)
                    dtNew.Rows.Add(drArr[j][0], drArr[j][dataType]);
                string dataStr = JsonConvert.SerializeObject(dtNew);
                dataStrArr[t] = "{\"series\":[{\"data\":" + dataStr + "}]}";
                dtNew.Clear();
            }

            HttpContext.Current.Response.Write("[" + string.Join(",", dataStrArr) + "]");
        }


        // 获取中国各省级行政区截至某日数据
        // dataType 用于选择数据类型 0-全部数据 1-累计确诊 2-现存确诊 3-累计治愈 4-累计死亡
        public static DataTable GetChinaProvinceDateData(string date, int dataType)
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
                  ORDER BY name ASC";

            DataTable dt = ExecuteSQL(sql);

            dt.Columns.Remove("name");
            dt.Columns.Remove("provinceEnglishName");
            dt.Columns.Remove("date");
            dt.Columns.Remove("updateTime");

            return dt;
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

            // 日期数据插值
            int i = 0;
            DateTime dt0 = Convert.ToDateTime("2020-01-22");
            DateTime dtn = Convert.ToDateTime(date);
            // 此时 i 指向 dt 第一行
            while (dt.Rows[0]["date"].ToString() != dt0.ToString("yyyy-MM-dd"))
            {
                DateTime dti0 = Convert.ToDateTime(dt.Rows[i]["date"].ToString()).AddDays(-1);
                DataRow dr = dt.NewRow();
                for (int t = 0; t < dt.Columns.Count; t++)
                {
                    if (t >= 1 && t <= 4)
                        dr[t] = 0;
                    else
                        dr[t] = dt.Rows[i].ItemArray[t];
                }
                dt.Rows.InsertAt(dr, 0);
                dt.Rows[0]["date"] = dti0.ToString("yyyy-MM-dd");
            }
            while (i < dt.Rows.Count - 1)
            {
                DateTime dti1 = Convert.ToDateTime(dt.Rows[i]["date"].ToString()).AddDays(1);
                DateTime dtj = Convert.ToDateTime(dt.Rows[i + 1]["date"].ToString());

                if (dtj != dti1)
                {
                    DataRow dr = dt.NewRow();
                    for (int t = 0; t < dt.Columns.Count; t++)
                        dr[t] = dt.Rows[i].ItemArray[t];
                    dt.Rows.InsertAt(dr, i + 1);
                    dt.Rows[i + 1]["date"] = dti1.ToString("yyyy-MM-dd");
                }

                i++;
            }
            // 此时 i 指向 dt 最后一行
            while (dt.Rows[i]["date"].ToString() != dtn.ToString("yyyy-MM-dd"))
            {
                DateTime dti1 = Convert.ToDateTime(dt.Rows[i]["date"].ToString()).AddDays(1);
                DataRow dr = dt.NewRow();
                for (int t = 0; t < dt.Columns.Count; t++)
                    dr[t] = dt.Rows[i].ItemArray[t];
                dt.Rows.InsertAt(dr, i + 1);
                dt.Rows[i + 1]["date"] = dti1.ToString("yyyy-MM-dd");
                i++;
            }

            if (dataType == 0)
            {
                string[] returnArr = new string[4];
                for (int j = 1; j <= 4; j++)
                {
                    List<long> valueY = dt.AsEnumerable().Select(d => d.Field<long>(j)).ToList();
                    returnArr[j - 1] = "{\"name\":\"" + dataTypeStr[j] + "\",\"type\":\"line\",\"data\":" + JsonConvert.SerializeObject(valueY) + "}";
                }
                HttpContext.Current.Response.Write("[" + string.Join(",", returnArr) + "]");
            }
            else
            {
                List<long> valueY = dt.AsEnumerable().Select(d => d.Field<long>("value")).ToList();
                HttpContext.Current.Response.Write("{\"name\":\"" + dataTypeStr[dataType] + "\",\"type\":\"line\",\"data\":" + JsonConvert.SerializeObject(valueY) + "}");
            }
        }


        [WebMethod(Description = "获取中国截至某日的时间序列数据")]
        // dataType 用于选择数据类型 0-全部数据 1-累计确诊 2-现存确诊 3-累计治愈 4-累计死亡
        public void GetChinaTimeSeriesData(string date, int dataType)
        {
            string[] dataTypeArr =
                { "t.province_confirmedCount, t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount, t.province_curedCount, t.province_deadCount",
                  "t.province_confirmedCount as value",
                  "t.province_confirmedCount - t.province_curedCount - t.province_deadCount as value",
                  "t.province_curedCount as value",
                  "t.province_deadCount as value"};

            string sql =
                  @"SELECT " + dataTypeArr[dataType] + @",
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM Global_Asia t
                     WHERE date <= '" + date + @"' AND t.provinceName = '中国'
                  GROUP BY date
                  ORDER BY date ASC";

            DataTable dt = ExecuteSQL(sql);

            dt.Columns.Remove("updateTime");

            // 日期数据插值
            int i = 0;
            DateTime dt0 = Convert.ToDateTime("2020-01-22");
            DateTime dtn = Convert.ToDateTime(date);
            // 此时 i 指向 dt 第一行
            while (dt.Rows[0]["date"].ToString() != dt0.ToString("yyyy-MM-dd"))
            {
                DateTime dti0 = Convert.ToDateTime(dt.Rows[i]["date"].ToString()).AddDays(-1);
                DataRow dr = dt.NewRow();
                DataTable dtTemp = GetChinaProvinceDateData(dti0.ToString("yyyy-MM-dd"), dataType);

                int col = 0;
                if (dataType == 0) col = 3;
                for (int t = 0; t < dt.Columns.Count; t++)
                {
                    if (t >= 0 && t <= col)
                    {
                        int totalTemp = 0;
                        for (int j = 0; j < dtTemp.Rows.Count; j++)
                            totalTemp += int.Parse(dtTemp.Rows[j][t].ToString());
                        dr[t] = totalTemp;
                    }
                    else
                        dr[t] = dt.Rows[i].ItemArray[t];
                }
                dt.Rows.InsertAt(dr, 0);
                dt.Rows[0]["date"] = dti0.ToString("yyyy-MM-dd");
            }
            while (i < dt.Rows.Count - 1)
            {
                DateTime dti1 = Convert.ToDateTime(dt.Rows[i]["date"].ToString()).AddDays(1);
                DateTime dtj = Convert.ToDateTime(dt.Rows[i + 1]["date"].ToString());

                if (dtj != dti1)
                {
                    DataRow dr = dt.NewRow();
                    DataTable dtTemp = GetChinaProvinceDateData(dti1.ToString("yyyy-MM-dd"), dataType);

                    int col = 0;
                    if (dataType == 0) col = 3;
                    for (int t = 0; t < dt.Columns.Count; t++)
                    {
                        if (t >= 0 && t <= col)
                        {
                            int totalTemp = 0;
                            for (int j = 0; j < dtTemp.Rows.Count; j++)
                                totalTemp += int.Parse(dtTemp.Rows[j][t].ToString());
                            dr[t] = totalTemp;
                        }
                        else
                            dr[t] = dt.Rows[i].ItemArray[t];
                    }
                    dt.Rows.InsertAt(dr, i + 1);
                    dt.Rows[i + 1]["date"] = dti1.ToString("yyyy-MM-dd");
                }

                i++;
            }
            // 此时 i 指向 dt 最后一行
            while (dt.Rows[i]["date"].ToString() != dtn.ToString("yyyy-MM-dd"))
            {
                DateTime dti1 = Convert.ToDateTime(dt.Rows[i]["date"].ToString()).AddDays(1);
                DataRow dr = dt.NewRow();
                DataTable dtTemp = GetChinaProvinceDateData(dti1.ToString("yyyy-MM-dd"), dataType);

                int col = 0;
                if (dataType == 0) col = 3;
                for (int t = 0; t < dt.Columns.Count; t++)
                {
                    if (t >= 0 && t <= col)
                    {
                        int totalTemp = 0;
                        for (int j = 0; j < dtTemp.Rows.Count; j++)
                            totalTemp += int.Parse(dtTemp.Rows[j][t].ToString());
                        dr[t] = totalTemp;
                    }
                    else
                        dr[t] = dt.Rows[i].ItemArray[t];
                }
                dt.Rows.InsertAt(dr, i + 1);
                dt.Rows[i + 1]["date"] = dti1.ToString("yyyy-MM-dd");
                i++;
            }

            if (dataType == 0)
            {
                string[] returnArr = new string[4];
                for (int j = 0; j <= 3; j++)
                {
                    List<long> valueY = dt.AsEnumerable().Select(d => d.Field<long>(j)).ToList();
                    returnArr[j] = "{\"name\":\"" + dataTypeStr[j + 1] + "\",\"type\":\"line\",\"data\":" + JsonConvert.SerializeObject(valueY) + "}";
                }
                HttpContext.Current.Response.Write("[" + string.Join(",", returnArr) + "]");
            }
            else
            {
                List<long> valueY = dt.AsEnumerable().Select(d => d.Field<long>("value")).ToList();
                HttpContext.Current.Response.Write("{\"name\":\"" + dataTypeStr[dataType] + "\",\"type\":\"line\",\"data\":" + JsonConvert.SerializeObject(valueY) + "}");
            }
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
