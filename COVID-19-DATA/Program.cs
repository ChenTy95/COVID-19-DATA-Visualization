using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Data.SQLite;
using System.Data;

namespace COVID_19_DATA
{
    class Program
    {
        static string ConnStrSQLite = "Data Source=COVID-19-DATA_" + DateTime.Now.ToString("yyyyMMddHHmmss") + ".db; Version=3";

        static void Main(string[] args)
        {
            Print("Press Any Key To Start...");
            Console.ReadKey();
            // 开始获取最新的 CSV 数据
            Print("Start Getting Latest COVID-19 Data...");
            int totalLength = GetLatestDXYAreaData(out string rawCSVDataStr);
            Print(string.Format("Finish! Raw Data Length = {0}", totalLength));
            // 将 CSV 数据转换至数据库
            ConvertCSV2SQLite(rawCSVDataStr, out string[] fieldName);
            // 分类整理数据库中数据
            DataProcessing(fieldName);
            // 将此次生成的文件设置为最新文件
            SetCurrentDatabaseFile();

            Console.ReadLine();
        }

        private static string GetCurrentTimeStamp()
        {
            return "[" + DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss.fff") + "] ";
        }

        // 控制台输出
        private static void Print(string str)
        {
            Console.WriteLine(GetCurrentTimeStamp() + str);
        }

        // 从 github 获取丁香园提供的最新地区疫情数据
        private static int GetLatestDXYAreaData(out string rawCSVData)
        {
            HttpWebRequest request = (HttpWebRequest)WebRequest.Create("https://raw.githubusercontent.com/BlankerL/DXY-COVID-19-Data/master/csv/DXYArea.csv");
            request.Method = "GET";
            request.Timeout = 10000;

            string responseStr;
            try
            {
                HttpWebResponse response = (HttpWebResponse)request.GetResponse();
                Stream myResponseStream = response.GetResponseStream();
                StreamReader myStreamReader = new StreamReader(myResponseStream, Encoding.UTF8);

                responseStr = myStreamReader.ReadToEnd();
                myStreamReader.Close();
                myResponseStream.Close();

                rawCSVData = responseStr.Trim();
            }
            catch (Exception ex)
            {
                rawCSVData = null;
                Console.WriteLine(ex.ToString());
                return -1;
            }

            if (string.IsNullOrEmpty(responseStr))
                return 0;
            else
                return responseStr.Length;
        }

        // SQLite 数据库建表，beginCol 从 1 开始数
        private static void CreateSQLiteTable(string[] fieldName, string tableName, int beginCol, int endCol)
        {
            string[] fieldType = new string[fieldName.Length];
            bool fieldNameOK = true;
            for (int i = beginCol - 1; i < endCol; i++)
            {
                if (fieldName[i].IndexOf("Name") > -1)
                    fieldType[i] = "TEXT";
                else if (fieldName[i].IndexOf("Count") > -1)
                    fieldType[i] = "INTEGER";
                else if (fieldName[i].IndexOf("zipCode") > -1)
                    fieldType[i] = "TEXT(6)";
                else if (fieldName[i].IndexOf("Time") > -1)
                    fieldType[i] = "TEXT(19)";
                else
                {
                    Print(string.Format("Strange Field Name: {0}", fieldName[i]));
                    fieldNameOK = false;
                }
            }
            if (!fieldNameOK)
            {
                Console.ReadLine();
                Environment.Exit(0);
            }
            else
            {
                string sqlCreate = "CREATE TABLE `" + tableName + "` (" + fieldName[beginCol - 1] + " " + fieldType[beginCol - 1];
                for (int i = beginCol; i < endCol; i++)
                    sqlCreate += ", " + fieldName[i] + " " + fieldType[i];
                sqlCreate += ")";

                SQLiteConnection conn = new SQLiteConnection(ConnStrSQLite);
                SQLiteCommand cmd = new SQLiteCommand(sqlCreate, conn);

                try
                {
                    conn.Open();
                    cmd.ExecuteNonQuery();
                    Print(string.Format("SQLite Database Create Table [{0}] OK!", tableName));
                }
                catch (Exception ex)
                {
                    Print(string.Format("SQLite Database Create Table [{0}] Error! Exception Message: {1}", tableName, ex.ToString()));
                    Console.ReadLine();
                    Environment.Exit(0);
                }
                finally
                {
                    conn.Close();
                }
            }
        }

        // 将 CSV 数据转换至 SQLite 数据库
        private static void ConvertCSV2SQLite(string rawCSVStr, out string[] fieldName)
        {
            string[] rawLineArr = rawCSVStr.Split(Environment.NewLine.ToCharArray());
            Print(string.Format("Raw CSVData Total Record = {0}", rawLineArr.Length));

            // 确定获取到的 CSV 数据字段数，如果不是 19，则提示后终止程序
            string[] dataFieldArr = rawLineArr[0].Split(',');
            fieldName = dataFieldArr;
            if (dataFieldArr.Length != 19)
            {
                Print(string.Format("Raw CSVData Field Num Error! [19] Expected, [{0}] Get.", rawLineArr.Length));
                Console.ReadLine();
                Environment.Exit(0);
            }

            CreateSQLiteTable(dataFieldArr, "RawCSVData", 1, dataFieldArr.Length);
            string sqlInsert = "INSERT INTO RawCSVData (" + dataFieldArr[0];
            for (int i = 1; i < dataFieldArr.Length; i++) sqlInsert += ", " + dataFieldArr[i];
            sqlInsert += ") VALUES (:" + dataFieldArr[0];
            for (int i = 1; i < dataFieldArr.Length; i++) sqlInsert += ", :" + dataFieldArr[i];
            sqlInsert += ")";

            SQLiteConnection conn = new SQLiteConnection(ConnStrSQLite);
            SQLiteCommand cmd = new SQLiteCommand(sqlInsert, conn);

            try
            {
                conn.Open();
                // 启用事务
                var transaction = conn.BeginTransaction();

                for (int i = 1; i < rawLineArr.Length; i++)
                {
                    // 处理新疆生产建设兵团英文名含半角逗号情况
                    if (rawLineArr[i].IndexOf(", Xinjiang") > -1)
                        rawLineArr[i] = rawLineArr[i].Replace(", Xinjiang", "@Xinjiang");

                    string[] data = rawLineArr[i].Split(',');
                    if (data.Length != dataFieldArr.Length)
                        Print(string.Format("Illegal Record {0}: {1}", i, string.Join(",", data)));
                    cmd.Parameters.Clear();
                    for (int j = 0; j < data.Length; j++)
                        cmd.Parameters.Add(new SQLiteParameter(dataFieldArr[j], data[j]));
                    cmd.ExecuteNonQuery();

                    if (i == 1 || i == rawLineArr.Length - 1 || i % (rawLineArr.Length / 100) == 0)
                    {
                        Print(string.Format("Insert Record {0}: {1}", i, string.Join(",", data)));
                    }
                }

                Print("Commit Transaction...");
                transaction.Commit();
                Print("Insert Data To SQLite Database OK!");
            }
            catch (Exception ex)
            {
                Print("Insert Data To SQLite Database Error! Exception Message: " + ex.ToString());
                Console.ReadLine();
                Environment.Exit(0);
            }
            finally
            {
                conn.Close();
            }
        }

        // 处理数据库数据，进行分类整理
        // 建立多个子表，按各大洲、中国各省、各省详细数据进行拆分
        private static void DataProcessing(string[] fieldName)
        {
            string sqlInsert;

            // 0.补全缺失数据
            CompleteMissingData();

            // 1.按各大洲分开
            DataTable dtContinent = ExecuteSQL("SELECT DISTINCT continentEnglishName FROM RawCSVData ORDER BY continentEnglishName");
            for (int i = 0; i < dtContinent.Rows.Count; i++)
            {
                string continentEnglishName = dtContinent.Rows[i]["continentEnglishName"].ToString();
                CreateSQLiteTable(fieldName, "Global_" + continentEnglishName, 5, 12);
                sqlInsert = string.Format(@"INSERT INTO `Global_{0}`
                                                 SELECT t.provinceName,
                                                        t.provinceEnglishName,
                                                        t.province_zipCode,
                                                        t.province_confirmedCount,
                                                        t.province_suspectedCount,
                                                        t.province_curedCount,
                                                        t.province_deadCount,
                                                        t.updateTime
                                                   FROM RawCSVData t
                                                  WHERE t.continentEnglishName = '{0}'
                                                    AND t.countryEnglishName = t.provinceEnglishName
                                                    AND t.countryName = t.provinceName
                                               ORDER BY provinceEnglishName, provinceName, updateTime", continentEnglishName);
                ExecuteSQLNonQuery(sqlInsert);
                Print(string.Format("-> Data Processing Completed: {0}", "Global_" + continentEnglishName));
            }

            // 2.中国各省数据整合（包括港澳台地区）
            CreateSQLiteTable(fieldName, "China", 5, 12);
            sqlInsert = @"INSERT INTO China
                               SELECT DISTINCT t.provinceName,
                                               t.provinceEnglishName,
                                               t.province_zipCode,
                                               t.province_confirmedCount,
                                               t.province_suspectedCount,
                                               t.province_curedCount,
                                               t.province_deadCount,
                                               t.updateTime
                                          FROM RawCSVData t
                                         WHERE (t.countryEnglishName = 'China' OR t.countryName = '中国') 
                                           AND t.provinceEnglishName <> 'China'
                                      ORDER BY provinceEnglishName, updateTime";
            ExecuteSQLNonQuery(sqlInsert);
            Print("-> Data Processing Completed: China");

            // 3.按中国各省分开（包括港澳台地区）
            DataTable dtChinaProvince = ExecuteSQL("SELECT DISTINCT t.provinceEnglishName FROM RawCSVData t WHERE t.countryEnglishName = 'China' ORDER BY t.provinceEnglishName");
            for (int i = 0; i < dtChinaProvince.Rows.Count; i++)
            {
                string provinceEnglishName = dtChinaProvince.Rows[i]["provinceEnglishName"].ToString();
                CreateSQLiteTable(fieldName, "China_" + provinceEnglishName, 12, 19);
                sqlInsert = string.Format(@"INSERT INTO `China_{0}`
                                                 SELECT t.updateTime,
                                                        t.cityName,
                                                        t.cityEnglishName,
                                                        t.city_zipCode,
                                                        t.city_confirmedCount,
                                                        t.city_suspectedCount,
                                                        t.city_curedCount,
                                                        t.city_deadCount
                                                   FROM RawCSVData t
                                                  WHERE t.provinceEnglishName = '{0}'
                                               ORDER BY city_zipCode, cityName, updateTime", provinceEnglishName);
                ExecuteSQLNonQuery(sqlInsert);
                Print(string.Format("-> Data Processing Completed: {0}", "China_" + provinceEnglishName));
            }
        }

        // 由于数据源有部分国家/地区数据不含 洲 信息，需要先补全
        private static void CompleteMissingData()
        {
            Print("Start Completing Missing Continent Data...");

            string sqlGetContinentInfo =
                  @"SELECT DISTINCT continentName,
                                    continentEnglishName,
                                    countryName,
                                    countryEnglishName,
                                    province_zipCode
                      FROM RawCSVData
                     WHERE province_zipCode IN (
                               SELECT DISTINCT t.province_zipCode
                                 FROM RawCSVData t
                                WHERE t.continentEnglishName = '' OR 
                                      t.continentName = ''
                           )
                       AND continentEnglishName <> ''
                       AND continentName <> ''";
            string sqlGetIncompleteData =
                  @"SELECT t.*
                      FROM RawCSVData t
                     WHERE continentEnglishName = '' OR 
                           continentName = ''";
            DataTable dtContinentInfo = ExecuteSQL(sqlGetContinentInfo);
            DataTable dtIncompleteRecord = ExecuteSQL(sqlGetIncompleteData);

            Print(string.Format("Missing Continent Data: {0} Records", dtIncompleteRecord.Rows.Count));

            for (int i = 0; i < dtIncompleteRecord.Rows.Count; i++)
            {
                string zipCode = dtIncompleteRecord.Rows[i]["province_zipCode"].ToString();
                DataRow dr = dtContinentInfo.Select("province_zipCode = '" + zipCode + "'")[0];
                dtIncompleteRecord.Rows[i][0] = dr[0];
                dtIncompleteRecord.Rows[i][1] = dr[1];
            }

            string sqlUpdate = @"UPDATE RawCSVData 
                                    SET continentName = :continentName, continentEnglishName = :continentEnglishName 
                                  WHERE province_zipCode = :province_zipCode 
                                    AND updateTime = :updateTime";

            SQLiteConnection conn = new SQLiteConnection(ConnStrSQLite);
            SQLiteCommand cmd = new SQLiteCommand(sqlUpdate, conn);

            try
            {
                conn.Open();
                var transaction = conn.BeginTransaction();

                for (int i = 0; i < dtIncompleteRecord.Rows.Count; i++)
                {
                    cmd.Parameters.Clear();
                    cmd.Parameters.Add(new SQLiteParameter("continentName", dtIncompleteRecord.Rows[i]["continentName"].ToString()));
                    cmd.Parameters.Add(new SQLiteParameter("continentEnglishName", dtIncompleteRecord.Rows[i]["continentEnglishName"].ToString()));
                    cmd.Parameters.Add(new SQLiteParameter("province_zipCode", dtIncompleteRecord.Rows[i]["province_zipCode"].ToString()));
                    cmd.Parameters.Add(new SQLiteParameter("updateTime", dtIncompleteRecord.Rows[i]["updateTime"].ToString()));

                    cmd.ExecuteNonQuery();
                }

                transaction.Commit();
                Print("Complete Missing Continent Data OK!");
            }
            catch (Exception ex)
            {
                Print("Complete Missing Continent Data Error! Exception Message: " + ex.ToString());
            }
            finally
            {
                conn.Close();
            }
        }

        // 执行 SQL 查询语句并返回 DataTable
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
                Print("SQL Execution Error! SQL: " + sql);
                Print("SQL Execution Error! Exception Message: " + ex.ToString());

                return null;
            }
            finally
            {
                conn.Close();
            }
        }

        // 执行 SQL 操作语句并返回影响行数
        private static int ExecuteSQLNonQuery(string sql)
        {
            SQLiteConnection conn = new SQLiteConnection(ConnStrSQLite);
            SQLiteCommand cmd = new SQLiteCommand(sql, conn);

            try
            {
                conn.Open();
                return cmd.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                Print("SQL Execution Error! SQL: " + sql);
                Print("SQL Execution Error! Exception Message: " + ex.ToString());

                return -1;
            }
            finally
            {
                conn.Close();
            }
        }

        // 将当前数据文件设置为有效文件
        private static void SetCurrentDatabaseFile()
        {
            // 0.获取当前目录和相应文件
            string path = System.Environment.CurrentDirectory;
            string fpath = path + "\\COVID-19-DATA.db";
            string fpathNew = path + "\\" + ConnStrSQLite.Substring(12, 31);
            // 判断是否存在 COVID-19-DATA.db 文件，存在则使用替换
            if (File.Exists(fpath))
            {
                File.Copy(fpathNew, fpath, true);
            }
            else
            {
                File.Copy(fpathNew, fpath);
            }

            Print("COVID-19-DATA.db Database File is Ready!");
        }
    }
}
