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
                dtTemp.Columns.Remove("name");

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
                    dtTemp.Columns.Remove("name");

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
                dtTemp.Columns.Remove("name");

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

        [WebMethod(Description = "获取中国省级行政区截至某日的各类数据")]
        // dataType 用于选择数据类型 0-全部数据 1-累计确诊 2-现存确诊 3-累计治愈 4-累计死亡
        public void GetEachProvinceDetailDateData(string provinceName, string date, int dataType)
        {
            string[] dataTypeStrTemp = { "", "累计确诊", "累计治愈", "现存确诊", "累计死亡" };

            string getEnglishNameSql = @"SELECT t.provinceEnglishName, max(t.updateTime) 
                                           FROM China t, Std_Name s
                                          WHERE s.standardName = '" + provinceName + "' AND t.provinceName = s.provinceName";
            DataTable dtEnglishName = ExecuteSQL(getEnglishNameSql);
            string provinceEnglishName = dtEnglishName.Rows[0][0].ToString();
            string[] specialArr = { "Hong Kong", "Macau", "Taiwan" };
            // 正常省份，从 China_[EnglishName] 表中取详细数据
            if (!(Array.IndexOf(specialArr, provinceEnglishName) > -1))
            {
                string[] dataTypeArr =
                { "t.city_confirmedCount, t.city_curedCount, t.city_confirmedCount - t.city_curedCount - t.city_deadCount as city_currentCount, t.city_deadCount",
                  "t.city_confirmedCount as value",
                  "t.city_confirmedCount - t.city_curedCount - t.city_deadCount as value",
                  "t.city_curedCount as value",
                  "t.city_deadCount as value"};

                string sql =
                      @"SELECT t.cityName as name, " + dataTypeArr[dataType] + @",
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM `China_" + provinceEnglishName + @"` t
                     WHERE  date <= '" + date + @"'
                  GROUP BY t.cityName
                  ORDER BY city_confirmedCount DESC";

                DataTable dt = ExecuteSQL(sql);

                for (int i = 0; i < dt.Rows.Count; i++)
                {
                    if (dt.Rows[i]["city_confirmedCount"].ToString() == "0" ||
                        dt.Rows[i]["city_confirmedCount"].ToString().Trim().Length == 0 ||
                        dt.Rows[i]["name"].ToString().IndexOf("监狱") > -1 ||
                        dt.Rows[i]["name"].ToString().IndexOf("未知") > -1 ||
                        dt.Rows[i]["name"].ToString().IndexOf("待明确") > -1 ||
                        dt.Rows[i]["name"].ToString().IndexOf("境外输入人员") > -1 ||
                        dt.Rows[i]["name"].ToString().IndexOf("市") > -1 ||
                        Convert.ToDateTime(dt.Rows[i]["date"].ToString()) < Convert.ToDateTime("2020-04-01"))
                    {
                        dt.Rows.RemoveAt(i);
                        i--;
                    }
                }
                dt.Columns.Remove("updateTime");
                dt.Columns.Remove("date");

                // 生成 X轴 名称数据
                List<string> valueX = dt.AsEnumerable().Select(d => d.Field<string>("name")).ToList();
                string xAxisData = JsonConvert.SerializeObject(valueX.ToArray());
                // 生成四类 Y轴 数据
                string[] returnArr = new string[4];
                for (int j = 1; j <= 4; j++)
                {
                    List<long> valueY = dt.AsEnumerable().Select(d => d.Field<long>(j)).ToList();
                    returnArr[j - 1] = "{\"name\":\"" + dataTypeStrTemp[j] + "\",\"type\":\"bar\",\"data\":" + JsonConvert.SerializeObject(valueY) + "}";
                }
                string seriesData = "[" + string.Join(",", returnArr) + "]";

                HttpContext.Current.Response.Write("[" + xAxisData + "," + seriesData + "]");
            }
            else
            {
                DataTable dtTemp = GetChinaProvinceDateData("2020-07-05", 0);
                dtTemp.Columns["province_curedCount"].SetOrdinal(2);
                DataRow dr = dtTemp.Select("name = '" + provinceName + "'")[0];
                string[] returnArr = new string[4];
                for (int j = 1; j <= 4; j++)
                {
                    returnArr[j - 1] = "{\"name\":\"" + dataTypeStrTemp[j] + "\",\"type\":\"bar\",\"data\":[" + dr[j].ToString() + "]}";
                }
                string seriesData = "[" + string.Join(",", returnArr) + "]";

                HttpContext.Current.Response.Write("[[\"" + provinceName + "\"]," + seriesData + "]");
            }
        }

        [WebMethod(Description = "获取世界各国截至某日的时间序列数据")]
        // dataType 用于选择数据类型 1-累计确诊 2-现存确诊 3-累计治愈 4-累计死亡
        public void GetWorldCountryTimeSeriesData(string date, int dataType)
        {
            // 先获取中国数据
            string[] dataTypeArr =
                { "t.province_confirmedCount, t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount, t.province_curedCount, t.province_deadCount",
                  "t.province_confirmedCount as value",
                  "t.province_confirmedCount - t.province_curedCount - t.province_deadCount as value",
                  "t.province_curedCount as value",
                  "t.province_deadCount as value"};

            string sql =
                  @"SELECT t.provinceName as name, " + dataTypeArr[dataType] + @",
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

                int col = 1;
                if (dataType == 0) col = 4;
                for (int t = 0; t < dt.Columns.Count; t++)
                {
                    if (t >= 1 && t <= col)
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

                    int col = 1;
                    if (dataType == 0) col = 4;
                    for (int t = 0; t < dt.Columns.Count; t++)
                    {
                        if (t >= 1 && t <= col)
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

                int col = 1;
                if (dataType == 0) col = 4;
                for (int t = 0; t < dt.Columns.Count; t++)
                {
                    if (t >= 1 && t <= col)
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
            DataTable dtChina = dt.Copy();

            // 获取其他国家数据，并进行日期插值
            sql =
                  @"SELECT t.provinceName as name,
                           t.provinceEnglishName,
                           t.province_confirmedCount,
                           t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount,
                           t.province_curedCount,
                           t.province_deadCount,
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM Global_Asia t
                     WHERE date <= '" + date + @"' AND t.provinceName <> '中国'
                     GROUP BY t.provinceName, substr(t.updateTime, 0, 11) 
                     UNION ALL
                    SELECT t.provinceName as name,
                           t.provinceEnglishName,
                           t.province_confirmedCount,
                           t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount,
                           t.province_curedCount,
                           t.province_deadCount,
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM Global_Africa t
                     WHERE date <= '" + date + @"' AND t.provinceEnglishName <> 'Reunion Island'
                     GROUP BY t.provinceName, substr(t.updateTime, 0, 11) 
                     UNION ALL
                    SELECT t.provinceName as name,
                           t.provinceEnglishName,
                           t.province_confirmedCount,
                           t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount,
                           t.province_curedCount,
                           t.province_deadCount,
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM Global_Europe t
                     WHERE date <= '" + date + @"' AND t.provinceEnglishName <> 'Georgia'
                     GROUP BY t.provinceName, substr(t.updateTime, 0, 11) 
                     UNION ALL
                    SELECT t.provinceName as name,
                           t.provinceEnglishName,
                           t.province_confirmedCount,
                           t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount,
                           t.province_curedCount,
                           t.province_deadCount,
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM `Global_North America` t
                     WHERE date <= '" + date + @"' AND t.provinceEnglishName <> 'Guam'
                     GROUP BY t.provinceName, substr(t.updateTime, 0, 11) 
                     UNION ALL
                    SELECT t.provinceName as name,
                           t.provinceEnglishName,
                           t.province_confirmedCount,
                           t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount,
                           t.province_curedCount,
                           t.province_deadCount,
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM `Global_South America` t
                     WHERE date <= '" + date + @"' AND t.provinceEnglishName <> 'St.Lucia' and t.provinceName <> '阿鲁巴'
                     GROUP BY t.provinceName, substr(t.updateTime, 0, 11) 
                     UNION ALL
                    SELECT t.provinceName as name,
                           t.provinceEnglishName,
                           t.province_confirmedCount,
                           t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount,
                           t.province_curedCount,
                           t.province_deadCount,
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM Global_Oceania t
                     WHERE date <= '" + date + @"'
                     GROUP BY t.provinceName, substr(t.updateTime, 0, 11) 
                     ORDER BY name ASC, date ASC";

            dt = ExecuteSQL(sql);

            dt.Columns.Remove("provinceEnglishName");
            dt.Columns.Remove("updateTime");

            // 日期数据插值
            i = 0;
            dt0 = Convert.ToDateTime("2020-01-22");
            dtn = Convert.ToDateTime(date);
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

            // 处理表 dt，删除无用列
            for (int t = 1; t < dataType; t++)
                dt.Columns.RemoveAt(1);
            for (int t = 1; t <= 4 - dataType; t++)
                dt.Columns.RemoveAt(2);
            dt.Columns[1].ColumnName = "value";

            // 合并两个表
            dt.Merge(dtChina);

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
                    dtNew.Rows.Add(drArr[j][0], drArr[j][1]);
                string dataStr = JsonConvert.SerializeObject(dtNew);
                dataStrArr[t] = "{\"series\":[{\"data\":" + dataStr + "}]}";
                dtNew.Clear();
            }

            HttpContext.Current.Response.Write("[" + string.Join(",", dataStrArr) + "]");
        }

        // 获取全球时间序列数据
        // dataType 用于选择数据类型 1-累计确诊 2-现存确诊 3-累计治愈 4-累计死亡
        private static DataTable GetWorldTimeSeriesDataTable(string date, int dataType)
        {
            // 先获取中国数据
            string[] dataTypeArr =
                { "t.province_confirmedCount, t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount, t.province_curedCount, t.province_deadCount",
                  "t.province_confirmedCount as value",
                  "t.province_confirmedCount - t.province_curedCount - t.province_deadCount as value",
                  "t.province_curedCount as value",
                  "t.province_deadCount as value"};

            string sql =
                  @"SELECT t.provinceName as name, " + dataTypeArr[dataType] + @",
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

                int col = 1;
                if (dataType == 0) col = 4;
                for (int t = 0; t < dt.Columns.Count; t++)
                {
                    if (t >= 1 && t <= col)
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

                    int col = 1;
                    if (dataType == 0) col = 4;
                    for (int t = 0; t < dt.Columns.Count; t++)
                    {
                        if (t >= 1 && t <= col)
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

                int col = 1;
                if (dataType == 0) col = 4;
                for (int t = 0; t < dt.Columns.Count; t++)
                {
                    if (t >= 1 && t <= col)
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
            DataTable dtChina = dt.Copy();

            // 获取其他国家数据，并进行日期插值
            sql =
                  @"SELECT t.provinceName as name,
                           t.provinceEnglishName,
                           t.province_confirmedCount,
                           t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount,
                           t.province_curedCount,
                           t.province_deadCount,
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM Global_Asia t
                     WHERE date <= '" + date + @"' AND t.provinceName <> '中国'
                     GROUP BY t.provinceName, substr(t.updateTime, 0, 11) 
                     UNION ALL
                    SELECT t.provinceName as name,
                           t.provinceEnglishName,
                           t.province_confirmedCount,
                           t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount,
                           t.province_curedCount,
                           t.province_deadCount,
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM Global_Africa t
                     WHERE date <= '" + date + @"' AND t.provinceEnglishName <> 'Reunion Island'
                     GROUP BY t.provinceName, substr(t.updateTime, 0, 11) 
                     UNION ALL
                    SELECT t.provinceName as name,
                           t.provinceEnglishName,
                           t.province_confirmedCount,
                           t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount,
                           t.province_curedCount,
                           t.province_deadCount,
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM Global_Europe t
                     WHERE date <= '" + date + @"' AND t.provinceEnglishName <> 'Georgia'
                     GROUP BY t.provinceName, substr(t.updateTime, 0, 11) 
                     UNION ALL
                    SELECT t.provinceName as name,
                           t.provinceEnglishName,
                           t.province_confirmedCount,
                           t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount,
                           t.province_curedCount,
                           t.province_deadCount,
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM `Global_North America` t
                     WHERE date <= '" + date + @"' AND t.provinceEnglishName <> 'Guam'
                     GROUP BY t.provinceName, substr(t.updateTime, 0, 11) 
                     UNION ALL
                    SELECT t.provinceName as name,
                           t.provinceEnglishName,
                           t.province_confirmedCount,
                           t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount,
                           t.province_curedCount,
                           t.province_deadCount,
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM `Global_South America` t
                     WHERE date <= '" + date + @"' AND t.provinceEnglishName <> 'St.Lucia' and t.provinceName <> '阿鲁巴'
                     GROUP BY t.provinceName, substr(t.updateTime, 0, 11) 
                     UNION ALL
                    SELECT t.provinceName as name,
                           t.provinceEnglishName,
                           t.province_confirmedCount,
                           t.province_confirmedCount - t.province_curedCount - t.province_deadCount as province_currentCount,
                           t.province_curedCount,
                           t.province_deadCount,
                           substr(t.updateTime, 0, 11) AS date,
                           max(t.updateTime) AS updateTime
                      FROM Global_Oceania t
                     WHERE date <= '" + date + @"'
                     GROUP BY t.provinceName, substr(t.updateTime, 0, 11) 
                     ORDER BY name ASC, date ASC";

            dt = ExecuteSQL(sql);

            dt.Columns.Remove("provinceEnglishName");
            dt.Columns.Remove("updateTime");

            // 日期数据插值
            i = 0;
            dt0 = Convert.ToDateTime("2020-01-22");
            dtn = Convert.ToDateTime(date);
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

            // 合并两个表
            dt.Merge(dtChina);

            return dt;
        }

        [WebMethod(Description = "获取世界总计截至某日的时间序列数据")]
        // dataType 用于选择数据类型 0-全部数据 1-累计确诊 2-现存确诊 3-累计治愈 4-累计死亡
        public void GetWorldTimeSeriesData(string date, int dataType)
        {
            DataTable dt = GetWorldTimeSeriesDataTable(date, dataType);

            int totalDays = (int)(Convert.ToDateTime(date) - Convert.ToDateTime("2020-01-22")).TotalDays;
            string[] dataStrArr = new string[totalDays + 1];
            DataTable dtNew = new DataTable();
            dtNew.Columns.Add("date", typeof(string));
            dtNew.Columns.Add("c1", typeof(int));
            dtNew.Columns.Add("c2", typeof(int));
            dtNew.Columns.Add("c3", typeof(int));
            dtNew.Columns.Add("c4", typeof(int));

            for (int t = 0; t <= totalDays; t++)
            {
                int[] value = { 0, 0, 0, 0 };
                string dtDay = Convert.ToDateTime("2020-01-22").AddDays(t).ToString("yyyy-MM-dd");
                DataRow[] drArr = dt.Select("date = '" + dtDay + "'");
                for (int j = 0; j < drArr.Length; j++)
                {
                    for (int k = 0; k < 4; k++)
                    {
                        value[k] += int.Parse(drArr[j][k + 1].ToString());
                    }
                }
                dtNew.Rows.Add(dtDay, value[0], value[1], value[2], value[3]);
            }

            string[] returnArr = new string[4];
            for (int i = 1; i <= 4; i++)
            {
                List<int> valueY = dtNew.AsEnumerable().Select(d => d.Field<int>(i)).ToList();
                returnArr[i - 1] = "{\"name\":\"" + dataTypeStr[i] + "\",\"type\":\"line\",\"data\":" + JsonConvert.SerializeObject(valueY) + "}";
            }

            HttpContext.Current.Response.Write("[" + string.Join(",", returnArr) + "]");
        }

        [WebMethod(Description = "获取世界上截至某日的累计确诊前 10 位国家及数据")]
        public void GetWorldLead10CountryData(string date)
        {
            DataTable dt = GetWorldTimeSeriesDataTable(date, 1);

            DataRow[] dr = dt.Select("date = '" + date + "'", "province_confirmedCount DESC");
            DataTable dtNew = dt.Clone();
            for (int i = 0; i < dr.Length; i++)
                dtNew.Rows.Add(dr[i].ItemArray);
            string[] returnCountry = new string[10];
            string[] returnArr = new string[10];
            for (int i = 0; i < 10; i++)
            {
                returnCountry[i] = dtNew.Rows[i][0].ToString();
                returnArr[i] = "{\"name\":\"" + dtNew.Rows[i][0].ToString() + "\",\"value\":\"" + dtNew.Rows[i][1].ToString() + "\"}";
            }
            HttpContext.Current.Response.Write("[" + JsonConvert.SerializeObject(returnCountry) + ",[" + string.Join(",", returnArr) + "]]");
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
