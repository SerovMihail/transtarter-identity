using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Web;
using System.Web.Hosting;
using System.Web.Services;
using IdentityModel.Client;
using TsApiServiceClient;
using TSServerDB.Managers;


namespace TSServerDB
{
    [WebService(Namespace = "http://tstarter.ru/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]
    // To allow this Web Service to be called from script, using ASP.NET AJAX, uncomment the following line. 
    [System.Web.Script.Services.ScriptService]
    public class Service : System.Web.Services.WebService
    {
        //<%@ WebService Language="C#" CodeBehind="~/App_Code/Service.cs" Class="TransStarter.WebService.Service" %>
        //<%@ WebService Language="C#" CodeBehind="TSWebService.asmx.cs" Class="TSServerDB.Service" %>
        private log4net.ILog mLogger;

        public Service()
        {
            var logPath =
                Path.Combine(HostingEnvironment.ApplicationPhysicalPath, "Logs");
            if (!Directory.Exists(logPath))
                Directory.CreateDirectory(logPath);

            log4net.GlobalContext.Properties["LogFileName"] = logPath;
            log4net.Config.XmlConfigurator.Configure();
            mLogger = log4net.LogManager.GetLogger("MethodDurabilityLogger");
        }

        #region Private Methods

        [WebMethod(EnableSession = true)]
        private int GetUserIDFromSession()
        {
            int result = 0;

            if (Session != null)
            {
                foreach (string key in Session.Keys)
                {
                    if (key == "partnerId")
                    {
                        if (Session[key] != null && (Session[key].ToString()).Length > 0 && Int32.TryParse(Session[key].ToString(), out result))
                        {
                            break;
                        }
                        else
                        {
                            result = 0;
                        }
                    }
                }
            }

            return result;
        }

        [WebMethod(EnableSession = true)]
        private SqlConnection GetDBConnection()
        {
            string connectionString;
            SqlConnection connTSC = null;

            try
            {
                connectionString = ConfigurationManager.AppSettings["MSQL"].ToString();
            }
            catch
            {
                connectionString = "";
            }

            if (connectionString.Length > 0)
            {
                connTSC = new SqlConnection(connectionString);
                try
                {
                    connTSC.Open();
                }
                catch (SqlException ex)
                {
                    string errorMessage = "";
                    foreach (SqlError ConnectionError in ex.Errors)
                    {
                        errorMessage += ConnectionError.Message + " (error: " + ConnectionError.Number.ToString() + ")" + (char)13;
                    }
                }
                if (!(connTSC != null && connTSC.State == ConnectionState.Open))
                {
                    connTSC = null;
                }
            }

            return connTSC;
        }

        [WebMethod(EnableSession = true)]
        private void WriteWebRequest(string number, int found_count)
        {
            int partner_id = GetUserIDFromSession(), in_current_rests_count = 0, in_history_rests_count = 0;
            if (partner_id != 0)
            {
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;
                        if (found_count == 1)
                        {
                            sqlcmd.CommandText = "SELECT COUNT(*) FROM (" +
                                "SELECT [Cross].ID, [Cross].CrossGroupID, [Cross].Number, GoodsRest.ID AS GoodsRestID,  GoodsRest.WarehouseID, " +
                                "ISNULL((SELECT AccessRight FROM dbo.PartnerClosedWarehouses WHERE WarehouseID = GoodsRest.WarehouseID AND " +
                                "PartnerID = " + partner_id.ToString() + "), 3) AS  AccessRight " +
                                "FROM dbo.[Cross] LEFT OUTER JOIN dbo.GoodsRest ON GoodsRest.Number = [Cross].Number " +
                                "WHERE [Cross].CrossGroupID IN (SELECT CrossGroupID FROM dbo.[Cross] WHERE Number = '" + number + "') " +
                                "AND GoodsRest.Rest > 0 AND GoodsRest.[Sign] = 'N' AND [Cross].Number NOT LIKE 'RTS%' " +
                                ") AS t WHERE t.AccessRight IN (1,2)";
                            try
                            {
                                in_current_rests_count = (int)sqlcmd.ExecuteScalar();
                            }
                            catch { }
                            sqlcmd.CommandText = "SELECT COUNT(*) FROM (" +
                                "SELECT [Cross].ID, [Cross].CrossGroupID, [Cross].Number, GoodsRest.ID AS GoodsRestID,  GoodsRest.WarehouseID, " +
                                "ISNULL((SELECT AccessRight FROM dbo.PartnerClosedWarehouses WHERE WarehouseID = GoodsRest.WarehouseID " +
                                "AND PartnerID = " + partner_id.ToString() + "), 3) AS  AccessRight " +
                                "FROM dbo.[Cross] LEFT OUTER JOIN dbo.GoodsRest ON GoodsRest.Number = [Cross].Number " +
                                "WHERE [Cross].CrossGroupID IN (SELECT CrossGroupID FROM dbo.[Cross] WHERE Number = '" + number + "') " +
                                " AND GoodsRest.Rest = 0 AND GoodsRest.[Sign] = 'N' AND [Cross].Number NOT LIKE 'RTS%' " +
                                ") AS t WHERE t.AccessRight IN (1,2)";
                            try
                            {
                                in_history_rests_count = (int)sqlcmd.ExecuteScalar();
                            }
                            catch { }
                        }
                        sqlcmd.CommandText = "INSERT INTO dbo.WebRequest (PartnerID, RequestDateTime, Number, SearchNumber, " +
                            "IsFound, IsInCurrentRests, IsInHistoryRests) VALUES (" + partner_id.ToString() + ", '" + DateTime.Now.ToString("MM.dd.yyyy HH:mm:ss") +
                            "', " + "'" + number + "', '" + number + "', " + (found_count > 0 ? "1, " : "0, ") + (in_current_rests_count > 0 ? "1, " : "0, ") +
                            (in_history_rests_count > 0 ? "1" : "0") + ")";
                        sqlcmd.ExecuteNonQuery();
                        //send data to new api
                        // request token
                        try
                        {
                            var identity = ConfigurationManager.AppSettings["Identity"];
                            var api = ConfigurationManager.AppSettings["TsApi"];

                            var tokenClient = new TokenClient(identity, "websiteapi", "secret");
                            var tokenResponse = tokenClient.RequestClientCredentialsAsync("tsapi").Result;

                            var httpClient = new HttpClient();
                            httpClient.SetBearerToken(tokenResponse.AccessToken);

                            var statClient = new TsApiServiceClient.Client(api, httpClient);

                            statClient.ApiStatisticPostAsync(new FullStatistic
                            {
                                Brand = "",
                                CompanyId = 1,
                                CreateDateTime = DateTime.Now,
                                CustomerId = partner_id,
                                IsFound = found_count > 0,
                                Number = number,
                                SearchNumber = number,
                                Login = "Tstarter.ru"
                            });
                        }
                        catch (Exception e)
                        {
                            Console.Write(e);
                            //
                        }




                    }
                }
            }
        }

        [WebMethod(EnableSession = true)]
        private bool IsAudit()
        {
            bool result = false;

            try
            {
                result = (ConfigurationManager.AppSettings["IsAudit"].ToString() == "1" ? true : false);

            }
            catch
            {
                result = false;
            }

            return result;
        }

        #endregion Private Methods

        #region Public Methods

        [WebMethod(EnableSession = true)]
        public string SetUser(string user, string password, string version)
        {
            var watch = Stopwatch.StartNew();

            DateTime startTime = DateTime.Now;
            string result = "False", externalUrl = "";
            int partner_id;
            bool show_article = false, show_prior_number = false, show_rest_number = false, show_cross_number = false,
                show_forthcoming = false, show_exact_rests = false, show_cart = false, show_order_history = false,
                show_unit_app = false, show_part_app = false, show_item_pic = false, show_all_crosses = false, show_aux_info = false;

            try
            {
                Session.Remove("partnerId");
            }
            catch { }
            //
            if (version != "1.2b")
            {
                result = "Доступна новая версия: [1.2b]";
            }
            else if (!String.IsNullOrEmpty(user) && !String.IsNullOrEmpty(password))
            {
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandText = "SELECT ISNULL((SELECT TOP (1) ID FROM dbo.[Partner] WHERE SitePassword = '" + password +
                            "' AND SiteLogin = '" + user + "' AND WebOrder = 1 ORDER BY ID DESC), 0)";
                        try
                        {
                            partner_id = (int)sqlcmd.ExecuteScalar();
                        }
                        catch
                        {
                            partner_id = 0;
                        }
                        if (partner_id > 0)
                        {
                            sqlcmd.CommandText = "SELECT COUNT(*) FROM dbo.Doc WHERE [Type] = 60 AND PartnerID = " + partner_id.ToString();
                            if ((int)sqlcmd.ExecuteScalar() > 0)
                            {
                                Session.Add("partnerId", partner_id);
                                Session.Add("userLogin", user);
                                //установить доступ пользователя к функциям веб-сервиса в соответствии с его группой пользователей для интернет-доступа
                                sqlcmd.CommandText = "SELECT ShowArticle, ShowPriorNumber, ShowRestNumber, ShowCrossNumber, ShowForthcoming, ShowExactRests, " +
                                    "Cart, OrderHistory, ShowUnitApplication, ShowPartApplication, ShowItemPicture FROM dbo.PartnerWebGroups WHERE ID = " +
                                    "(SELECT ISNULL((SELECT PartnerWebGroupsID FROM dbo.[Partner] WHERE ID = " +
                                    partner_id.ToString() + "), 0))";
                                using (SqlDataReader r = sqlcmd.ExecuteReader())
                                {
                                    if (r.HasRows)
                                    {
                                        r.Read();
                                        show_article = r.IsDBNull(0) ? false : (bool)r.GetSqlBoolean(0);
                                        show_prior_number = r.IsDBNull(1) ? false : (bool)r.GetSqlBoolean(1);
                                        show_rest_number = r.IsDBNull(2) ? false : (bool)r.GetSqlBoolean(2);
                                        show_cross_number = r.IsDBNull(3) ? false : (bool)r.GetSqlBoolean(3);
                                        show_forthcoming = r.IsDBNull(4) ? false : (bool)r.GetSqlBoolean(4);
                                        show_exact_rests = r.IsDBNull(5) ? false : (bool)r.GetSqlBoolean(5);
                                        show_cart = r.IsDBNull(6) ? false : (bool)r.GetSqlBoolean(6);
                                        show_order_history = r.IsDBNull(7) ? false : (bool)r.GetSqlBoolean(7);
                                        show_unit_app = r.IsDBNull(8) ? false : (bool)r.GetSqlBoolean(8);
                                        show_part_app = r.IsDBNull(9) ? false : (bool)r.GetSqlBoolean(9);
                                        show_item_pic = r.IsDBNull(10) ? false : (bool)r.GetSqlBoolean(10);
                                    }
                                }
                                Session.Add("ShowArticle", show_article);
                                Session.Add("ShowPriorNumber", show_prior_number);
                                Session.Add("ShowRestNumber", show_rest_number);
                                Session.Add("ShowForthcoming", show_forthcoming);
                                Session.Add("ShowExactRests", show_exact_rests);
                                Session.Add("ShowCart", show_cart);
                                Session.Add("ShowOrderHistory", show_order_history);
                                Session.Add("ShowUnitApplication", show_unit_app);
                                Session.Add("ShowPartApplication", show_part_app);
                                Session.Add("ShowItemPicture", show_item_pic);
                                //
                                sqlcmd.CommandText = "SELECT ShowAllCrosses, ShowAuxInfo FROM dbo.[Partner] WHERE ID = " + partner_id.ToString();
                                using (SqlDataReader r = sqlcmd.ExecuteReader())
                                {
                                    if (r.HasRows)
                                    {
                                        r.Read();
                                        show_all_crosses = (r.IsDBNull(0) ? 0 : (byte)r.GetByte(0)) == 0 ? false : true;
                                        show_aux_info = (r.IsDBNull(1) ? 0 : (byte)r.GetByte(1)) == 0 ? false : true;
                                    }
                                }
                                Session.Add("ShowAuxInfo", show_aux_info);
                                if (!show_cross_number)
                                {
                                    Session.Add("ShowCrossNumber", show_cross_number);
                                    Session.Add("OriginalShowCrossNumber", show_cross_number);
                                }
                                else
                                {
                                    Session.Add("ShowCrossNumber", show_all_crosses);
                                    Session.Add("OriginalShowCrossNumber", show_cross_number);
                                }
                                //
                                externalUrl = ConfigurationManager.AppSettings["ExternalWebServiceURL"].ToString();
                                Session.Add("externalUrl", externalUrl);
                                Session.Add("externalLogin", (ConfigurationManager.AppSettings["ExternalLogin"] ?? "").ToString());
                                Session.Add("externalPassword", (ConfigurationManager.AppSettings["ExternalPassword"] ?? "").ToString());
                                //
                                result = "True";
                            }
                            else
                            {
                                result = "False";
                            }
                        }
                        else
                        {
                            result = "False";
                        }

                        if (IsAudit())
                        {
                            sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, Comment) VALUES (100, 'SetUser', " +
                                partner_id.ToString() + ", " + Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                ", '" + result.ToString() + ";" + user + ":" + password + "')";
                            sqlcmd.ExecuteNonQuery();
                        }
                    }
                    connTSC.Close();
                    connTSC.Dispose();
                }
            }
            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");

            return result;
        }

        [WebMethod(EnableSession = true)]
        public bool GetAccessAttribute(string attribute_name)
        {
            var watch = Stopwatch.StartNew();
            bool result = false;

            if (attribute_name.Length > 0 && Session != null && GetUserIDFromSession() > 0)
            {
                if (Session[attribute_name] != null)
                {
                    try
                    {
                        Boolean.TryParse(Session[attribute_name].ToString(), out result);
                    }
                    catch
                    {
                        result = false;
                    }
                }
            }
            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");
            return result;
        }

        [WebMethod(EnableSession = true)]
        public DataSet FindNumberInCross(string number)
        {
            var watch = Stopwatch.StartNew();
            DateTime startTime = DateTime.Now;
            string query, externalUrl;
            DataSet dataSet = new DataSet();

            if (!String.IsNullOrEmpty(number) && GetUserIDFromSession() != 0)
            {
                try
                {
                    externalUrl = Session["externalUrl"].ToString();
                }
                catch
                {
                    externalUrl = "";
                }
                if (!string.IsNullOrEmpty(externalUrl))
                {
                    try
                    {
                        using (ExternalTSWS.Service extSvc = new ExternalTSWS.Service() { Url = externalUrl })
                        {
                            dataSet = extSvc.FindNumberInCrossForExternalCall(number,
                                (Session["externalLogin"] ?? "").ToString(), (Session["externalPassword"] ?? "").ToString());
                        }
                    }
                    catch (Exception ex) { }
                }
                else
                {
                    SqlConnection connTSC = GetDBConnection();
                    if (connTSC != null && connTSC.State == ConnectionState.Open)
                    {
                        number = number.Replace("'", "");
                        number = number.Replace(" ", "").Replace(".", "").Replace("-", "").Replace(",", "").Replace(@"\", "").Replace("/", "");

                        query = "SELECT t.Number, t.Manufacturer, t.CrossGroupID, " +
                            "CASE WHEN (t.[Description] IS NULL) OR (LEN(t.[Description]) = 0) THEN " +
                            "ISNULL((SELECT TOP (1) [Description] FROM dbo.CrossComment WHERE CrossGroupID = t.CrossGroupID ORDER BY ID DESC), '') " +
                            "ELSE t.[Description] END AS [Description] FROM (" +
                            "SELECT TOP(200) Number, Manufacturer, CrossGroupID, " +
                            "(SELECT TOP(1) [Description] FROM dbo.Goods WHERE Number = [Cross].Number ORDER BY ID DESC) AS [Description] " +
                            "FROM dbo.[Cross] WHERE SearchNumber LIKE '%" + number + "%' AND Number NOT LIKE 'RTS%')" +
                            " AS t ORDER BY t.Number, t.Manufacturer";
                        /*
                         old code:
                                                "SELECT TOP(200) Number, Manufacturer, CrossGroupID, " +
                                                    "(SELECT TOP(1) [Description] FROM dbo.Goods WHERE Number = [Cross].Number ORDER BY ID DESC) AS [Description] " +
                                                    "FROM dbo.[Cross] WHERE SearchNumber LIKE '%" + number + "%' AND Number NOT LIKE 'RTS%' " +
                                                    "ORDER BY Number, Manufacturer";
                        */
                        try
                        {
                            SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                            dataAdapter.Fill(dataSet, "Table");
                            //
                            WriteWebRequest(number, dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0);
                        }
                        catch { }

                        if (IsAudit())
                        {
                            try
                            {
                                using (SqlCommand sqlcmd = new SqlCommand())
                                {
                                    sqlcmd.Connection = connTSC;
                                    sqlcmd.CommandType = CommandType.Text;
                                    sqlcmd.CommandTimeout = 7200;
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, Number, NewOutQty, GoodsRestID) VALUES (100, 'FindNumberInCross', " +
                                        GetUserIDFromSession().ToString() + ", '" + (number.Length > 20 ? number.Substring(0, 20) : number) + "', " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                    sqlcmd.ExecuteNonQuery();
                                }
                            }
                            catch { }
                        }

                        connTSC.Close();
                        connTSC.Dispose();
                    }
                }
            }
            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");
            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet FindNumber(string number, bool only)
        {
            var watch = Stopwatch.StartNew();

            DateTime startTime = DateTime.Now;
            string query, externalUrl;
            DataSet dataSet = new DataSet();

            if (!String.IsNullOrEmpty(number) && GetUserIDFromSession() != 0)
            {
                try
                {
                    externalUrl = Session["externalUrl"].ToString();
                }
                catch
                {
                    externalUrl = "";
                }
                if (!string.IsNullOrEmpty(externalUrl))
                {
                    try
                    {
                        using (ExternalTSWS.Service extSvc = new ExternalTSWS.Service() { Url = externalUrl })
                        {
                            dataSet = extSvc.FindNumberForExternalCall(number, only,
                                (Session["externalLogin"] ?? "").ToString(), (Session["externalPassword"] ?? "").ToString());
                        }
                    }
                    catch (Exception ex) { }
                }
                else
                {
                    SqlConnection connTSC = GetDBConnection();
                    if (connTSC != null && connTSC.State == ConnectionState.Open)
                    {
                        number = number.Replace(" ", "").Replace(".", "").Replace("-", "").Replace(",", "").Replace(@"\", "").Replace("/", "");

                        query = "SELECT t.Number, t.Manufacturer, t.CrossGroupID, " +
                            "CASE WHEN (t.[Description] IS NULL) OR (LEN(t.[Description]) = 0) THEN " +
                            "ISNULL((SELECT TOP (1) [Description] FROM dbo.CrossComment WHERE CrossGroupID = t.CrossGroupID ORDER BY ID DESC), '') " +
                            "ELSE t.[Description] END AS [Description] FROM (" +
                            "SELECT TOP(200) Number, Manufacturer, CrossGroupID, " +
                            "(SELECT TOP(1) [Description] FROM dbo.Goods WHERE Number = [Cross].Number ORDER BY ID DESC) AS [Description] " +
                            "FROM dbo.[Cross] WHERE SearchNumber " + (only ? "= '" + number + "'" : "LIKE '%" + number + "%'") +
                            " AND Number NOT LIKE 'RTS%') AS t ORDER BY t.Number, t.Manufacturer";
                        try
                        {
                            SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                            dataAdapter.Fill(dataSet, "Table");
                            //
                            WriteWebRequest(number, dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0);
                        }
                        catch { }

                        if (IsAudit())
                        {
                            try
                            {
                                using (SqlCommand sqlcmd = new SqlCommand())
                                {
                                    sqlcmd.Connection = connTSC;
                                    sqlcmd.CommandType = CommandType.Text;
                                    sqlcmd.CommandTimeout = 7200;
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, Number, NewOutQty, GoodsRestID) VALUES (100, 'FindNumber', " +
                                        GetUserIDFromSession().ToString() + ", '" + (number.Length > 20 ? number.Substring(0, 20) : number) + "', " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                    sqlcmd.ExecuteNonQuery();
                                }
                            }
                            catch { }
                        }

                        connTSC.Close();
                        connTSC.Dispose();
                    }
                }
            }

            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");
            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet FindInGoodsRest(int crossGroupID)
        {
            var watch = Stopwatch.StartNew();

            DateTime startTime = DateTime.Now;
            string currency_short_name, query, externalUrl, partner_name = "";
            DataSet dataSet = new DataSet();
            int partner_id = GetUserIDFromSession(), aux_sort = 0;
            bool data_for_number_exists = false, show_cross_number = false, show_exact_rests = false,
                show_part_application = false, show_forthcoming = false, is_white_green_number = false,
                set_data_for_number_without_rest = false;
            double price = 0, price_eur = 0;

            if (crossGroupID > 0 && partner_id != 0)
            {
                try
                {
                    externalUrl = Session["externalUrl"].ToString();
                }
                catch
                {
                    externalUrl = "";
                }
                if (!string.IsNullOrEmpty(externalUrl))
                {
                    show_cross_number = GetAccessAttribute("ShowCrossNumber");
                    show_exact_rests = GetAccessAttribute("ShowExactRests");
                    show_part_application = GetAccessAttribute("ShowPartApplication");
                    show_forthcoming = GetAccessAttribute("ShowForthcoming");
                    //
                    DataTable dataTable = new DataTable("TempTable");
                    dataTable.Columns.Add("Manufacturer", typeof(string));
                    dataTable.Columns.Add("Number", typeof(string));
                    dataTable.Columns.Add("Description", typeof(string));
                    dataTable.Columns.Add("Price", typeof(double));
                    dataTable.Columns.Add("PriceEUR", typeof(double));
                    if (show_exact_rests)
                    {
                        dataTable.Columns.Add("Amount", typeof(double));
                    }
                    dataTable.Columns.Add("NewSell", typeof(int));
                    dataTable.Columns.Add("WarehouseID", typeof(int));
                    dataTable.Columns.Add("WarehouseName", typeof(string));
                    dataTable.Columns.Add("AmountComponents", typeof(int));
                    dataTable.Columns.Add("AccessRight", typeof(byte));
                    dataTable.Columns.Add("OnStore", typeof(int));
                    if (show_forthcoming)
                    {
                        dataTable.Columns.Add("ForthcomingInfo", typeof(string));
                    }
                    dataTable.Columns.Add("ViewID", typeof(string));
                    dataTable.Columns.Add("RoublePrice", typeof(double));
                    dataTable.Columns.Add("AuxSort", typeof(int));

                    dataTable.DefaultView.Sort = "AuxSort ASC, Price ASC";

                    /*
                    //build test data
                    dataRow = dataTable.NewRow();
                    dataRow["Manufacturer"] = "TS";
                    dataRow["Number"] = "TS0482";
                    dataRow["Description"] = "Бендикс";
                    dataRow["Price"] = 262.8;
                    dataRow["PriceEUR"] = 3.6;
                    if (show_exact_rests)
                    {
                        dataRow["Amount"] = 896;
                    }
                    dataRow["NewSell"] = 0;
                    dataRow["WarehouseID"] = 1;
                    dataRow["WarehouseName"] = "СКЛАД МСК";
                    dataRow["AmountComponents"] = 0;
                    dataRow["AccessRight"] = 1;
                    dataRow["OnStore"] = 1;
                    if (show_forthcoming)
                    {
                        dataRow["ForthcomingInfo"] = "";
                    }
                    dataRow["ViewID"] = "0x100008483485256501839625200000108690";
                    dataRow["RoublePrice"] = 262.8;
                    dataTable.Rows.Add(dataRow);

                    dataRow = dataTable.NewRow();
                    dataRow["Manufacturer"] = "WAI";
                    dataRow["Number"] = "WA54-9140";
                    dataRow["Description"] = "Бендикс";
                    dataRow["Price"] = 343.1;
                    dataRow["PriceEUR"] = 4.7;
                    if (show_exact_rests)
                    {
                        dataRow["Amount"] = 267;
                    }
                    dataRow["NewSell"] = 0;
                    dataRow["WarehouseID"] = 1;
                    dataRow["WarehouseName"] = "СКЛАД МСК";
                    dataRow["AmountComponents"] = 0;
                    dataRow["AccessRight"] = 1;
                    dataRow["OnStore"] = 1;
                    if (show_forthcoming)
                    {
                        dataRow["ForthcomingInfo"] = "";
                    }
                    dataRow["ViewID"] = "0x100008765535245574952482401732900000101405";
                    dataRow["RoublePrice"] = 343.1;
                    dataTable.Rows.Add(dataRow);

                    dataRow = dataTable.NewRow();
                    dataRow["Manufacturer"] = "BOSCH";
                    dataRow["Number"] = "1006209503";
                    dataRow["Description"] = "Бендикс";
                    dataRow["Price"] = 519.03;
                    dataRow["PriceEUR"] = 7.12;
                    if (show_exact_rests)
                    {
                        dataRow["Amount"] = 3;
                    }
                    dataRow["NewSell"] = 0;
                    dataRow["WarehouseID"] = 1;
                    dataRow["WarehouseName"] = "СКЛАД МСК";
                    dataRow["AmountComponents"] = 0;
                    dataRow["AccessRight"] = 1;
                    dataRow["OnStore"] = 1;
                    if (show_forthcoming)
                    {
                        dataRow["ForthcomingInfo"] = "";
                    }
                    dataRow["ViewID"] = "0x10000494848545048575348513633259840000111846";
                    dataRow["RoublePrice"] = 519.03;
                    dataTable.Rows.Add(dataRow);
                    */

                    dataSet.AcceptChanges();

                    try
                    {
                        using (ExternalTSWS.Service extSvc = new ExternalTSWS.Service() { Url = externalUrl })
                        {
                            SqlConnection connTSC = GetDBConnection();
                            if (connTSC != null && connTSC.State == ConnectionState.Open)
                            {
                                using (SqlCommand sqlcmd = new SqlCommand())
                                {
                                    sqlcmd.Connection = connTSC;
                                    sqlcmd.CommandType = CommandType.Text;
                                    sqlcmd.CommandTimeout = 7200;
                                    sqlcmd.CommandText = "SELECT ISNULL((SELECT Name FROM dbo.[Partner] WHERE ID = " +
                                        partner_id.ToString() + "), '')";
                                    partner_name = (string)sqlcmd.ExecuteScalar();
                                }
                            }
                            //
                            DataSet mndDataSet = extSvc.GetExternalMNDList(crossGroupID,
                                (Session["externalLogin"] ?? "").ToString(), (Session["externalPassword"] ?? "").ToString());
                            if (mndDataSet != null && mndDataSet.Tables.Count > 0)
                            {
                                foreach (DataRow mndDataRow in mndDataSet.Tables[0].Rows)
                                {
                                    data_for_number_exists = false;
                                    set_data_for_number_without_rest = false;
                                    if (connTSC != null && connTSC.State == ConnectionState.Open &&
                                        !DBNull.Value.Equals(mndDataRow["Number"]))
                                    {
                                        using (SqlCommand sqlcmd = new SqlCommand(), sqlcmd2 = new SqlCommand())
                                        {
                                            sqlcmd2.Connection = connTSC;
                                            sqlcmd2.CommandType = CommandType.Text;
                                            sqlcmd2.CommandTimeout = 7200;
                                            //
                                            sqlcmd.Connection = connTSC;
                                            sqlcmd.CommandType = CommandType.Text;
                                            sqlcmd.CommandTimeout = 7200;
                                            sqlcmd.CommandText =
                                                "SELECT t4.Number, t4.Price, t4.PriceEUR, t4.WarehouseID, t4.WarehouseName, t4.Amount, t4.AccessRight " +
                                                //
                                                (show_forthcoming ?
                                                ", (SELECT CASE WHEN (SELECT COUNT(*) FROM dbo.Forthcoming " +
                                                "WHERE Number = t4.Number AND (Qty - ReservedQty) > 0" +  // <- [Type] = 1020 AND : 10.02 
                                                                                                          //new code 25.11
                                                " AND ShipmentID IN (SELECT DISTINCT ShipmentID FROM dbo.Forthcoming WHERE IsVisible = 1)" +  // <- AND Type = 1020 : 10.02
                                                " AND WarehouseID IN (SELECT WarehouseID FROM dbo.PartnerClosedWarehouses WHERE AccessRight IN (1,2) AND PartnerID = " + partner_id.ToString() + ")" +
                                                //end of new code 25.11
                                                ") > 0 THEN 'да' ELSE '' END) AS ForthcomingInfo " : "") +
                                                //
                                                ", dbo.udfGetAggregatedID(t4.Number, t4.Price, t4.PriceEUR, t4.WarehouseID) " +
                                                //
                                                "FROM (" +
                                                "SELECT t3.Number, t3.Price, t3.PriceEUR, t3.WarehouseID, t3.WarehouseName, t3.Amount," +
                                                "ISNULL((SELECT AccessRight FROM dbo.PartnerClosedWarehouses WHERE WarehouseID = t3.WarehouseID AND PartnerID = "
                                                + partner_id.ToString() + "), 3) AS AccessRight " +
                                                "FROM (SELECT t2.Number, t2.Price, t2.PriceEUR, t2.WarehouseID, t2.WarehouseName, SUM(t2.Rest) AS Amount " +
                                                "FROM (SELECT t.Number, " +
                                                "dbo.udfGetWebPartnerRestPrice(1, 1, 1, '" + partner_name + "' , t.GoodsRestID, ISNULL(dbo.udfReturnCurrencyRate(GETDATE()), 0)) AS Price, " +
                                                "dbo.udfGetWebPartnerRestPrice(1, 1, 1, '" + partner_name + "', t.GoodsRestID, 1) AS PriceEUR, " +
                                                "t.Rest, t.WarehouseID, " +
                                                "ISNULL((SELECT Name FROM dbo.Warehouse WHERE ID = t.WarehouseID), '') AS WarehouseName " +
                                                "FROM (" +
                                                "SELECT [Cross].ID, [Cross].CrossGroupID, [Cross].Number, GoodsRest.ID AS GoodsRestID,  GoodsRest.WarehouseID, GoodsRest.Rest, " +
                                                "ISNULL((SELECT TOP 1 ShowInClientPrice FROM dbo.Goods WHERE Number = [Cross].Number ORDER BY ID DESC), 1) AS ShowInClientPrice " +
                                                "FROM dbo.[Cross] LEFT OUTER JOIN dbo.GoodsRest ON GoodsRest.Number = [Cross].Number " +
                                                "WHERE [Cross].Number = '" + mndDataRow["Number"] +
                                                "' AND GoodsRest.Rest > 0 AND GoodsRest.[Sign] = 'N' AND [Cross].Number NOT LIKE 'RTS%' " +
                                                ") AS t WHERE t.ShowInClientPrice = 1) AS t2 " +
                                                "GROUP BY t2.Number, t2.Price, t2.PriceEUR, t2.WarehouseID, t2.WarehouseName " +
                                                ") AS t3) AS t4 WHERE t4.AccessRight IN (1,2) ORDER BY t4.Price, t4.Number";
                                            using (SqlDataReader r = sqlcmd.ExecuteReader())
                                            {
                                                if (r.HasRows)
                                                {
                                                    data_for_number_exists = true;
                                                    while (r.Read())
                                                    {
                                                        DataRow dataRow = dataTable.NewRow();
                                                        dataRow["Manufacturer"] = mndDataRow["Manufacturer"].ToString();
                                                        dataRow["Number"] = mndDataRow["Number"].ToString();
                                                        dataRow["Description"] = mndDataRow["Description"].ToString();
                                                        dataRow["Price"] = r.IsDBNull(1) ? 0 : (double)r.GetSqlDouble(1);
                                                        dataRow["PriceEUR"] = r.IsDBNull(2) ? 0 : (double)r.GetSqlDouble(2);
                                                        if (show_exact_rests)
                                                        {
                                                            dataRow["Amount"] = r.IsDBNull(5) ? 0 : (double)r.GetSqlDouble(5);
                                                        }
                                                        dataRow["NewSell"] = 0;
                                                        dataRow["WarehouseID"] = r.IsDBNull(3) ? 0 : (int)r.GetSqlInt32(3);
                                                        dataRow["WarehouseName"] = r.IsDBNull(4) ? "" : (string)r.GetSqlString(4);
                                                        if (show_part_application)
                                                        {
                                                            dataRow["AmountComponents"] = mndDataRow["AmountComponents"];
                                                        }
                                                        else
                                                        {
                                                            dataRow["AmountComponents"] = 0;
                                                        }
                                                        dataRow["AccessRight"] = r.IsDBNull(6) ? 0 : (byte)r.GetSqlByte(6);
                                                        if ((r.IsDBNull(5) ? 0 : (double)r.GetSqlDouble(5)) > 0)
                                                        {
                                                            dataRow["OnStore"] = 1;
                                                        }
                                                        else
                                                        {
                                                            dataRow["OnStore"] = 0;
                                                        }
                                                        if (show_forthcoming)
                                                        {
                                                            dataRow["ForthcomingInfo"] = r.IsDBNull(7) ? "" : (string)r.GetSqlString(7);
                                                        }
                                                        dataRow["ViewID"] = r.IsDBNull(8) ? "" : (string)r.GetSqlString(8);
                                                        dataRow["RoublePrice"] = r.IsDBNull(1) ? 0 : (double)r.GetSqlDouble(1);
                                                        dataRow["AuxSort"] = 1;
                                                        dataTable.Rows.Add(dataRow);
                                                    }
                                                }
                                            }
                                            if (!data_for_number_exists)
                                            {
                                                price = 0;
                                                price_eur = 0;
                                                sqlcmd2.CommandText = "SELECT " +
                                                    "dbo.udfGetWebPartnerNumberPrice(1, 1, '" + partner_name + "', '" + mndDataRow["Number"] + "', ISNULL(dbo.udfReturnCurrencyRate(GETDATE()), 0)) AS Price, " +
                                                    "dbo.udfGetWebPartnerNumberPrice(1, 1, '" + partner_name + "', '" + mndDataRow["Number"] + "', 1) AS PriceEUR";
                                                using (SqlDataReader r = sqlcmd2.ExecuteReader())
                                                {
                                                    if (r.HasRows)
                                                    {
                                                        r.Read();
                                                        price = r.IsDBNull(0) ? 0 : (double)r.GetSqlDouble(0);
                                                        price_eur = r.IsDBNull(1) ? 0 : (double)r.GetSqlDouble(1);
                                                    }
                                                }
                                                //
                                                if (!show_cross_number)
                                                {
                                                    //ShowCrossNumber = 0: - show position from "white" and "green" references only, including ones without rest
                                                    sqlcmd2.CommandText = "SELECT " +
                                                        "ISNULL((SELECT COUNT(*) FROM dbo.GoodsRest WHERE Number = '" + mndDataRow["Number"].ToString() + "'),0) + " +
                                                        "ISNULL((SELECT COUNT(*) FROM dbo.Goods WHERE Number = '" + mndDataRow["Number"].ToString() + "'),0)";
                                                    if ((int)sqlcmd2.ExecuteScalar() > 0)
                                                    {
                                                        set_data_for_number_without_rest = true;
                                                    }
                                                }
                                                else
                                                {
                                                    set_data_for_number_without_rest = true;
                                                }
                                                if (set_data_for_number_without_rest)
                                                {
                                                    DataRow dataRow = dataTable.NewRow();
                                                    dataRow["Manufacturer"] = mndDataRow["Manufacturer"].ToString();
                                                    dataRow["Number"] = mndDataRow["Number"].ToString();
                                                    dataRow["Description"] = mndDataRow["Description"].ToString();
                                                    //
                                                    dataRow["Price"] = price;
                                                    dataRow["PriceEUR"] = price_eur;
                                                    dataRow["RoublePrice"] = price;
                                                    if (price != 0 || price_eur != 0)
                                                    {
                                                        dataRow["AuxSort"] = 2;
                                                    }
                                                    else
                                                    {
                                                        dataRow["AuxSort"] = 3;
                                                    }
                                                    //
                                                    if (show_exact_rests)
                                                    {
                                                        dataRow["Amount"] = 0;
                                                    }
                                                    dataRow["NewSell"] = 0;
                                                    dataRow["WarehouseID"] = 0;
                                                    dataRow["WarehouseName"] = "";
                                                    if (show_part_application)
                                                    {
                                                        dataRow["AmountComponents"] = mndDataRow["AmountComponents"];
                                                    }
                                                    else
                                                    {
                                                        dataRow["AmountComponents"] = 0;
                                                    }
                                                    dataRow["AccessRight"] = 0;
                                                    dataRow["OnStore"] = 0;
                                                    if (show_forthcoming)
                                                    {
                                                        sqlcmd2.CommandText =
                                                            "SELECT CASE WHEN (SELECT COUNT(*) FROM dbo.Forthcoming " +
                                                            "WHERE Number = '" + mndDataRow["Number"].ToString() + "' AND (Qty - ReservedQty) > 0 " + // <- [Type] = 1020 AND : 10.02 
                                                                                                                                                      //new code 25.11
                                                            " AND ShipmentID IN (SELECT DISTINCT ShipmentID FROM dbo.Forthcoming WHERE IsVisible = 1) " + // <- AND Type = 1020 : 10.02
                                                            " AND WarehouseID IN (SELECT WarehouseID FROM dbo.PartnerClosedWarehouses WHERE AccessRight IN (1,2) AND PartnerID = " +
                                                            partner_id.ToString() + ")" +
                                                            //end of new code 25.11
                                                            ") > 0 THEN 'да' ELSE '' END";
                                                        dataRow["ForthcomingInfo"] = (string)sqlcmd2.ExecuteScalar();
                                                    }
                                                    //
                                                    sqlcmd2.CommandText = "SELECT dbo.udfGetAggregatedID('" + mndDataRow["Number"].ToString() +
                                                        "', " + price.ToString().Replace(",", ".") + ", " +
                                                        price_eur.ToString().Replace(",", ".") + ", 0)";
                                                    dataRow["ViewID"] = (string)sqlcmd2.ExecuteScalar();
                                                    //
                                                    dataTable.Rows.Add(dataRow);
                                                }
                                            }
                                        }
                                    }
                                }

                                //-- "… про сортировку в он-лайне…
                                //-- Надо так: вверху все что есть в наличии и при этом сгруппировано по складам (по ID склада), а потом по ценам (вверху самое дешевое).
                                //aux_sort = 1
                                //
                                //4
                                //-- Затем идет все что без остатков отсортировано по цене (вверху самое дешевое)
                                //aux_sort = 2
                                //
                                //-- Затем все что без цен."
                                //aux_sort = 3
                                //
                                //ORDER BY AuxSort, Price

                                DataTable resultTable = dataTable.DefaultView.ToTable("Table");
                                dataSet.Tables.Add(resultTable);
                                dataSet.AcceptChanges();
                            }
                        }
                    }
                    catch (Exception ex) { }
                    dataSet.Tables[0].Columns.Remove("AuxSort");
                    dataSet.AcceptChanges();
                }
                else
                {
                    SqlConnection connTSC = GetDBConnection();
                    if (connTSC != null && connTSC.State == ConnectionState.Open)
                    {
                        using (SqlCommand sqlcmd = new SqlCommand())
                        {
                            sqlcmd.Connection = connTSC;
                            sqlcmd.CommandType = CommandType.Text;
                            sqlcmd.CommandTimeout = 7200;
                            sqlcmd.CommandText = "SELECT ISNULL((SELECT CurrencyShortName FROM dbo.[Partner] WHERE ID = " + partner_id.ToString() + "), '')";
                            try
                            {
                                currency_short_name = (string)sqlcmd.ExecuteScalar();
                            }
                            catch
                            {
                                currency_short_name = "руб";
                            }
                        }
                        query = "SELECT t.Manufacturer, t.Number, t.[Description], " + (currency_short_name == "руб" ? "t.Price" : "t.PriceEUR") + " AS Price, " +
                            "t.PriceEUR, " + (GetAccessAttribute("ShowExactRests") ? "t.Amount, " : "") + " t.NewSell, " +
                            "t.WarehouseID, t.WarehouseName, " + (GetAccessAttribute("ShowPartApplication") ? "t.AmountComponents" : "0") +
                            ", t.AccessRight, t.OnStore" + //-- , t.ID
                                                           //
                            (GetAccessAttribute("ShowForthcoming") ? ", (SELECT CASE WHEN (SELECT COUNT(*) FROM dbo.Forthcoming " +
                            "WHERE Number = t.Number AND (Qty - ReservedQty) > 0" +  // <- [Type] = 1020 AND : 10.02
                                                                                     //new code 25.11
                            " AND ShipmentID IN (SELECT DISTINCT ShipmentID FROM dbo.Forthcoming WHERE IsVisible = 1)" +  // <- AND Type = 1020 : 10.02
                            " AND WarehouseID IN (SELECT WarehouseID FROM dbo.PartnerClosedWarehouses WHERE AccessRight IN (1,2) AND PartnerID = " + partner_id.ToString() + ")" +
                            //end of new code 25.11
                            ") > 0 THEN 'да' ELSE '' END ) AS ForthcomingInfo" : "") + ", t.ViewID, t.Price AS RoublePrice " +
                            //
                            "FROM (SELECT Manufacturer, Number, [Description], Price AS Price, " +
                            "PriceEUR, Amount, NewSell, WarehouseID, WarehouseName, AmountComponents, AccessRight, OnStore, ID, ViewID " +
                            "FROM dbo.udfFindInGoodsRest(" + partner_id.ToString() + ", " + crossGroupID.ToString() + ", " +
                            (GetAccessAttribute("ShowCrossNumber") ? "1" : "0") + ", " + (GetAccessAttribute("ShowForthcoming") ? "1" : "0") +
                            ")) AS t ORDER BY t.ID";

                        try
                        {
                            SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                            dataAdapter.Fill(dataSet, "Table");
                        }
                        catch { }

                        if (IsAudit())
                        {
                            try
                            {
                                using (SqlCommand sqlcmd = new SqlCommand())
                                {
                                    sqlcmd.Connection = connTSC;
                                    sqlcmd.CommandType = CommandType.Text;
                                    sqlcmd.CommandTimeout = 7200;
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, DocStrID, NewOutQty, GoodsRestID) VALUES (100, 'FindInGoodsRest', " +
                                        GetUserIDFromSession().ToString() + ", " + crossGroupID.ToString() + ", " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                    sqlcmd.ExecuteNonQuery();
                                }
                            }
                            catch { }
                        }

                        connTSC.Close();
                        connTSC.Dispose();
                    }
                }
            }

            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");
            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet FindInDocStr(int docID)
        {
            var watch = Stopwatch.StartNew();

            DateTime startTime = DateTime.Now;
            string query;
            DataSet dataSet = new DataSet();
            int partner_id = GetUserIDFromSession();
            string currency_short_name = "EUR";

            if (docID > 0 && GetUserIDFromSession() != 0)
            {
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText = "SELECT ISNULL((SELECT CurrencyShortName FROM dbo.Doc WHERE ID = " +
                            docID.ToString() + "), 'EUR')";
                        try
                        {
                            currency_short_name = (string)sqlcmd.ExecuteScalar();
                        }
                        catch
                        {
                            currency_short_name = "EUR";
                        }
                    }
                    query = "SELECT ReserveDate, [Description], PriorNumber, Number, Qty, OutQty, ";
                    if (currency_short_name == "руб")
                    {
                        query += "dbo.udfGetDocStrOutAuxPriceWithVAT(DocStr.ID) AS OutPrice, " +
                            "dbo.udfGetDocStrOutAuxSumWithVAT(DocStr.ID) AS summa ";
                    }
                    else
                    {
                        query += "dbo.udfGetDocStrOutPriceWithVAT(DocStr.ID) AS OutPrice, " +
                            "dbo.udfGetDocStrOutSumWithVAT(DocStr.ID) AS summa ";
                    }
                    query += "FROM dbo.DocStr WHERE DocID = " + docID.ToString();
                    try
                    {
                        SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                        dataAdapter.Fill(dataSet, "Table");
                    }
                    catch { }

                    if (IsAudit())
                    {
                        try
                        {
                            using (SqlCommand sqlcmd = new SqlCommand())
                            {
                                sqlcmd.Connection = connTSC;
                                sqlcmd.CommandType = CommandType.Text;
                                sqlcmd.CommandTimeout = 7200;
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, DocStrID, NewOutQty, GoodsRestID) VALUES (100, 'FindInDocStr', " +
                                    GetUserIDFromSession().ToString() + ", " + docID.ToString() + ", " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                sqlcmd.ExecuteNonQuery();
                            }
                        }
                        catch { }
                    }

                    connTSC.Close();
                    connTSC.Dispose();
                }
            }
            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public bool GetCartEnabled()
        {
            var watch = Stopwatch.StartNew();

            DateTime startTime = DateTime.Now;
            bool result = false;
            int partner_id = GetUserIDFromSession();

            if (GetAccessAttribute("ShowCart") && partner_id != 0)
            {
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText = "SELECT COUNT(*) FROM dbo.Doc WHERE [Type] = 60 AND PartnerID = " + partner_id.ToString();
                        if ((int)sqlcmd.ExecuteScalar() > 0)
                        {
                            result = true;
                        }
                        else
                        {
                            result = false;
                        }
                    }

                    if (IsAudit())
                    {
                        try
                        {
                            using (SqlCommand sqlcmd = new SqlCommand())
                            {
                                sqlcmd.Connection = connTSC;
                                sqlcmd.CommandType = CommandType.Text;
                                sqlcmd.CommandTimeout = 7200;
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, Comment) VALUES (100, 'GetCartEnabled', " +
                                    GetUserIDFromSession().ToString() + ", " + Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    ", '" + result.ToString() + "')";
                                sqlcmd.ExecuteNonQuery();
                            }
                        }
                        catch { }
                    }

                    connTSC.Close();
                    connTSC.Dispose();
                }
            }
            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");

            return result;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetCart()
        {
            var watch = Stopwatch.StartNew();
            DateTime startTime = DateTime.Now;
            string currency_short_name, query;
            DataSet dataSet = new DataSet();
            int partner_id = GetUserIDFromSession();

            if (GetAccessAttribute("ShowCart") && partner_id != 0)
            {
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText = "SELECT ISNULL((SELECT CurrencyShortName FROM dbo.[Partner] WHERE ID = " + partner_id.ToString() + "), '')";
                        try
                        {
                            currency_short_name = (string)sqlcmd.ExecuteScalar();
                        }
                        catch
                        {
                            currency_short_name = "руб";
                        }
                    }
                    //
                    query = "SELECT ID, ReserveDate, [Description], PriorNumber, Number, " +
                        //
                        "dbo.udfGetWebOutAuxPrice(0, 1, 1, " + partner_id.ToString() + ", DocStr.OutPrice, DocStr.GoodsRestID, " +
                        (currency_short_name == "руб" ? "dbo.udfReturnCurrencyRate(GETDATE())" : "1") + ") AS OutPrice, " +
                        //
                        "OutPrice AS OutPriceEUR, OutQty AS Qty, OutQty, " +
                        //
                        "(dbo.udfGetWebOutAuxPrice(0, 1, 1, " + partner_id.ToString() + ", DocStr.OutPrice, DocStr.GoodsRestID, " +
                        (currency_short_name == "руб" ? "dbo.udfReturnCurrencyRate(GETDATE())" : "1") + ") * OutQty) AS summa " +
                        //new code (warehouse and warehouse group):
                        ", (SELECT  ISNULL(WarehouseID, 0) FROM dbo.GoodsRest WHERE ID = DocStr.GoodsRestID) AS WarehouseID, " +
                        "(SELECT Name FROM dbo.Warehouse WHERE ID = (SELECT  ISNULL(WarehouseID, 0) FROM dbo.GoodsRest WHERE " +
                        "ID = DocStr.GoodsRestID)) AS WarehouseName, (SELECT TOP 1 WarehouseGroupID FROM dbo.Warehouse WHERE " +
                        "ID IN (SELECT  ISNULL(WarehouseID, 0) FROM dbo.GoodsRest WHERE ID = DocStr.GoodsRestID)) AS WarehouseGroupID, " +
                        "(SELECT Name FROM dbo.WarehouseGroup WHERE ID = (SELECT TOP 1 WarehouseGroupID FROM dbo.Warehouse WHERE " +
                        "ID IN (SELECT ISNULL(WarehouseID, 0) FROM dbo.GoodsRest WHERE ID = DocStr.GoodsRestID)) ) AS WarehouseGroupName, " +
                        "ISNULL(IsForOrder, 0) AS IsForOrder " +
                        //
                        "FROM dbo.DocStr WHERE DocID IN (SELECT TOP 1 ID FROM dbo.Doc WHERE [Type] = 60 AND PartnerID = " +
                        partner_id.ToString() + " ORDER BY ID DESC)";
                    try
                    {
                        SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                        dataAdapter.Fill(dataSet, "Table");
                    }
                    catch { }

                    if (IsAudit())
                    {
                        try
                        {
                            using (SqlCommand sqlcmd = new SqlCommand())
                            {
                                sqlcmd.Connection = connTSC;
                                sqlcmd.CommandType = CommandType.Text;
                                sqlcmd.CommandTimeout = 7200;
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, GoodsRestID) VALUES (100, 'GetCart', " +
                                    GetUserIDFromSession().ToString() + ", " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                sqlcmd.ExecuteNonQuery();
                            }
                        }
                        catch { }
                    }

                    connTSC.Close();
                    connTSC.Dispose();
                }
            }
            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");
            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public int AddToCart(string number, int amount, decimal outPrice, string description)
        {
            int result = 0;

            //...

            return result;
        }

        [WebMethod(EnableSession = true)]
        public int AddToCartNew(string number, int amount, decimal outPrice, string description, int warehouseID)
        {
            var watch = Stopwatch.StartNew();
            DateTime startTime = DateTime.Now;
            int result = 0, doc_id = 0, partner_id = GetUserIDFromSession();

            if (partner_id != 0)
            {
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText = "SELECT ISNULL((SELECT TOP 1 ID FROM dbo.Doc WHERE Type = 60 AND PartnerID = " +
                            partner_id.ToString() + " ORDER BY ID DESC), 0)";
                        try
                        {
                            doc_id = (int)sqlcmd.ExecuteScalar();
                        }
                        catch
                        {
                            doc_id = 0;
                        }
                        if (doc_id > 0)
                        {
                            //списать выбранный товар по его номеру с выбранного склада по FIFO
                            sqlcmd.CommandType = CommandType.StoredProcedure;
                            sqlcmd.CommandText = "spAddToCartNew";
                            sqlcmd.Parameters.Clear();
                            //
                            SqlParameter prm_doc_id = new SqlParameter();
                            prm_doc_id.Direction = ParameterDirection.Input;
                            prm_doc_id.ParameterName = "@doc_id";
                            prm_doc_id.SqlDbType = SqlDbType.Int;
                            prm_doc_id.Value = doc_id;
                            sqlcmd.Parameters.Add(prm_doc_id);
                            //
                            SqlParameter prm_partner_id = new SqlParameter();
                            prm_partner_id.Direction = ParameterDirection.Input;
                            prm_partner_id.ParameterName = "@partner_id";
                            prm_partner_id.SqlDbType = SqlDbType.Int;
                            prm_partner_id.Value = partner_id;
                            sqlcmd.Parameters.Add(prm_partner_id);
                            //
                            SqlParameter prm_number = new SqlParameter();
                            prm_number.Direction = ParameterDirection.Input;
                            prm_number.ParameterName = "@number";
                            prm_number.SqlDbType = SqlDbType.VarChar;
                            prm_number.Value = number;
                            sqlcmd.Parameters.Add(prm_number);
                            //
                            SqlParameter prm_amount = new SqlParameter();
                            prm_amount.Direction = ParameterDirection.Input;
                            prm_amount.ParameterName = "@amount";
                            prm_amount.SqlDbType = SqlDbType.Int;
                            prm_amount.Value = amount;
                            sqlcmd.Parameters.Add(prm_amount);
                            //
                            SqlParameter prm_out_price = new SqlParameter();
                            prm_out_price.Direction = ParameterDirection.Input;
                            prm_out_price.ParameterName = "@out_price";
                            prm_out_price.SqlDbType = SqlDbType.Float;
                            prm_out_price.Value = outPrice;
                            sqlcmd.Parameters.Add(prm_out_price);
                            //
                            SqlParameter prm_description = new SqlParameter();
                            prm_description.Direction = ParameterDirection.Input;
                            prm_description.ParameterName = "@description";
                            prm_description.SqlDbType = SqlDbType.VarChar;
                            prm_description.Value = description;
                            sqlcmd.Parameters.Add(prm_description);
                            //
                            SqlParameter prm_warehouse_id = new SqlParameter();
                            prm_warehouse_id.Direction = ParameterDirection.Input;
                            prm_warehouse_id.ParameterName = "@warehouse_id";
                            prm_warehouse_id.SqlDbType = SqlDbType.Int;
                            prm_warehouse_id.Value = warehouseID;
                            sqlcmd.Parameters.Add(prm_warehouse_id);
                            //
                            SqlParameter prm_result = new SqlParameter();
                            prm_result.Direction = ParameterDirection.Output;
                            prm_result.ParameterName = "@result";
                            prm_result.SqlDbType = SqlDbType.Int;
                            sqlcmd.Parameters.Add(prm_result);
                            try
                            {
                                sqlcmd.ExecuteNonQuery();
                                result = (int)prm_result.Value;
                            }
                            catch
                            {
                                result = 0;
                            }
                        }

                        if (IsAudit())
                        {
                            try
                            {
                                sqlcmd.CommandType = CommandType.Text;
                                sqlcmd.Parameters.Clear();
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, GoodsRestID, Comment) VALUES (100, 'AddToCartNew', " +
                                    GetUserIDFromSession().ToString() + ", " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    ", " + result.ToString() + ", '" +
                                    number + ", " + amount.ToString() + ", " + outPrice.ToString("F2").Replace(",", ".") + ", " + description + ", " + warehouseID.ToString() + "')";
                                sqlcmd.ExecuteNonQuery();
                            }
                            catch { }
                        }

                    }
                    connTSC.Close();
                    connTSC.Dispose();
                }
            }
            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");
            return result;
        }

        [WebMethod(EnableSession = true)]
        public void DeleteFromCart(int id)
        {

            DateTime startTime = DateTime.Now;
            int partner_id = GetUserIDFromSession();

            if (partner_id != 0)
            {
                var watch = Stopwatch.StartNew();
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.StoredProcedure;
                        sqlcmd.CommandText = "spDeleteFromCart";
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.Parameters.Clear();
                        //
                        SqlParameter doc_str_id = new SqlParameter();
                        doc_str_id.Direction = ParameterDirection.Input;
                        doc_str_id.ParameterName = "@doc_str_id";
                        doc_str_id.SqlDbType = SqlDbType.Int;
                        doc_str_id.Value = id;
                        sqlcmd.Parameters.Add(doc_str_id);
                        //
                        sqlcmd.ExecuteNonQuery();
                        //
                        if (IsAudit())
                        {
                            try
                            {
                                sqlcmd.CommandType = CommandType.Text;
                                sqlcmd.Parameters.Clear();
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, GoodsRestID) VALUES (100, 'DeleteFromCart', " +
                                    GetUserIDFromSession().ToString() + ", " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    ", " + id.ToString() + ")";
                                sqlcmd.ExecuteNonQuery();
                            }
                            catch { }
                        }
                    }
                    connTSC.Close();
                    connTSC.Dispose();
                }
                watch.Stop();
                if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                    mLogger.Info($"{watch.ElapsedMilliseconds}");
            }
        }

        [WebMethod(EnableSession = true)]
        public void OrderCreate(int warehouseGroupID, string clientComment)
        {
            DateTime startTime = DateTime.Now;
            int partner_id = GetUserIDFromSession();

            if (partner_id != 0)
            {
                var watch = Stopwatch.StartNew();
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    if (clientComment.Length > 200)
                    {
                        clientComment = clientComment.Substring(0, 199);
                    }
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.StoredProcedure;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText = "spWebOrderCreate";
                        sqlcmd.Parameters.Clear();
                        //
                        SqlParameter prm_partner_id = new SqlParameter();
                        prm_partner_id.Direction = ParameterDirection.Input;
                        prm_partner_id.ParameterName = "@partner_id";
                        prm_partner_id.SqlDbType = SqlDbType.Int;
                        prm_partner_id.Value = partner_id;
                        sqlcmd.Parameters.Add(prm_partner_id);
                        //
                        SqlParameter prm_warehouse_group_id = new SqlParameter();
                        prm_warehouse_group_id.Direction = ParameterDirection.Input;
                        prm_warehouse_group_id.ParameterName = "@warehouse_group_id";
                        prm_warehouse_group_id.SqlDbType = SqlDbType.Int;
                        prm_warehouse_group_id.Value = warehouseGroupID;
                        sqlcmd.Parameters.Add(prm_warehouse_group_id);
                        //
                        SqlParameter prm_client_comment = new SqlParameter();
                        prm_client_comment.Direction = ParameterDirection.Input;
                        prm_client_comment.ParameterName = "@client_comment";
                        prm_client_comment.SqlDbType = SqlDbType.VarChar;
                        prm_client_comment.Value = clientComment;
                        sqlcmd.Parameters.Add(prm_client_comment);
                        //
                        try
                        {
                            sqlcmd.ExecuteNonQuery();
                        }
                        catch
                        { }

                        if (IsAudit())
                        {
                            try
                            {
                                sqlcmd.CommandType = CommandType.Text;
                                sqlcmd.CommandTimeout = 7200;
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, GoodsRestID, Comment) VALUES (100, 'OrderCreate', " +
                                    GetUserIDFromSession().ToString() + ", " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    ", " + warehouseGroupID.ToString() + ", '" + clientComment + "')";
                                sqlcmd.ExecuteNonQuery();
                            }
                            catch { }
                        }

                    }
                    connTSC.Close();
                    connTSC.Dispose();
                }
                watch.Stop();
                if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                    mLogger.Info($"{watch.ElapsedMilliseconds}");
            }
        }

        [WebMethod(EnableSession = true)]
        public decimal GetCurrencyRate()
        {
            DateTime startTime = DateTime.Now;
            decimal result = 0;

            if (GetUserIDFromSession() != 0)
            {
                var watch = Stopwatch.StartNew();
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText = "SELECT ISNULL(dbo.udfReturnCurrencyRate(GETDATE()), 0)";
                        try
                        {
                            result = Convert.ToDecimal((double)sqlcmd.ExecuteScalar());
                        }
                        catch
                        {
                            result = 0;
                        }

                        if (IsAudit())
                        {
                            try
                            {
                                sqlcmd.CommandType = CommandType.Text;
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, NewInPrice) VALUES (100, 'GetCurrencyRate', " +
                                    GetUserIDFromSession().ToString() + ", " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    ", " + result.ToString().Replace(",", ".") + ")";
                                sqlcmd.ExecuteNonQuery();
                            }
                            catch { }
                        }
                    }
                    connTSC.Close();
                    connTSC.Dispose();
                }
                watch.Stop();
                if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                    mLogger.Info($"{watch.ElapsedMilliseconds}");
            }

            return result;
        }

        [WebMethod(EnableSession = true)]
        public string GetCurrency()
        {
            DateTime startTime = DateTime.Now;
            string result = "";
            int partner_id = GetUserIDFromSession();

            if (partner_id != 0)
            {
                var watch = Stopwatch.StartNew();
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText = "SELECT ISNULL((SELECT CurrencyShortName FROM dbo.[Partner] WHERE ID = " + partner_id.ToString() + "), '')";
                        try
                        {
                            result = (string)sqlcmd.ExecuteScalar();
                        }
                        catch
                        {
                            result = "";
                        }

                        if (IsAudit())
                        {
                            try
                            {
                                sqlcmd.CommandType = CommandType.Text;
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, Comment) VALUES (100, 'GetCurrency', " +
                                    GetUserIDFromSession().ToString() + ", " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    ", '" + result + "')";
                                sqlcmd.ExecuteNonQuery();
                            }
                            catch { }
                        }
                    }
                    connTSC.Close();
                    connTSC.Dispose();
                }
                watch.Stop();
                if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                    mLogger.Info($"{watch.ElapsedMilliseconds}");
            }

            return result;
        }

        [WebMethod(EnableSession = true)]
        public decimal GetCredit()
        {
            DateTime startTime = DateTime.Now;
            decimal result = 0;

            int partner_id = GetUserIDFromSession();

            if (partner_id != 0)
            {
                var watch = Stopwatch.StartNew();
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText = "SELECT ISNULL((SELECT Credit FROM dbo.[Partner] WHERE ID = " + partner_id.ToString() + "), 0)";
                        try
                        {
                            result = Convert.ToDecimal((double)sqlcmd.ExecuteScalar());
                        }
                        catch
                        {
                            result = 0;
                        }
                        if (IsAudit())
                        {
                            try
                            {
                                sqlcmd.CommandType = CommandType.Text;
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, NewInPrice) VALUES (100, 'GetCredit', " +
                                    GetUserIDFromSession().ToString() + ", " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    "," + result.ToString().Replace(",", ".") + ")";
                                sqlcmd.ExecuteNonQuery();
                            }
                            catch { }
                        }
                    }
                    connTSC.Close();
                    connTSC.Dispose();
                }
                watch.Stop();
                if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                    mLogger.Info($"{watch.ElapsedMilliseconds}");
            }

            return result;
        }

        [WebMethod(EnableSession = true)]
        public DateTime GetCurrentDate()
        {
            return DateTime.Today;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetOrders(DateTime dateBegin, DateTime dateEnd, bool showAll)
        {
            DateTime startTime = DateTime.Now;
            string query, str_date1 = "", str_date2 = "", date_clause = "";
            DataSet dataSet = new DataSet();
            int partner_id = GetUserIDFromSession();

            if (GetAccessAttribute("ShowOrderHistory") && partner_id != 0)
            {
                var watch = Stopwatch.StartNew();
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    try
                    {
                        str_date1 = dateBegin.ToString("MM-dd-yyyy") + " 00:00:00";
                    }
                    catch
                    {
                        str_date1 = "";
                    }
                    try
                    {
                        str_date2 = dateEnd.ToString("MM-dd-yyyy") + " 23:59:59";
                    }
                    catch
                    {
                        str_date2 = "";
                    }
                    if (str_date1.Length > 0 && str_date2.Length > 0)
                    {
                        date_clause = " AND (dbo.udfGetOutDocDate(Doc.ID) >= '" + str_date1 + "' AND dbo.udfGetOutDocDate(Doc.ID) <= '" + str_date2 + "')";
                    }
                    else if (str_date1.Length > 0)
                    {
                        date_clause = " AND (dbo.udfGetOutDocDate(Doc.ID) >= '" + str_date1 + "')";
                    }
                    else if (str_date2.Length > 0)
                    {
                        date_clause = " AND (dbo.udfGetOutDocDate(Doc.ID) <= '" + str_date2 + "')";
                    }
                    //
                    query = "SELECT ID,  dbo.udfGetOutDocDate(Doc.ID) AS [Date], Number, [Type], " +
                        "CASE [Type] WHEN 10 THEN 'Приход' " +
                        "WHEN 12 THEN 'Возврат товара от клиента' " +
                        "WHEN 20 THEN 'Наряд-заказ' WHEN 21 THEN 'Розничная продажа' " +
                        "WHEN 22 THEN 'Розничная продажа' WHEN 23 THEN 'Расход товара на клиента, выполненный через интерфейс `Заказы клентов`' " +
                        "WHEN 26 THEN 'Расход товара на клиента, выполненный через web-заказы' " +
                        "WHEN 27 THEN 'Возврат товара от клиента' WHEN 41 THEN 'Безналичный платёж' " +
                        "WHEN 42 THEN 'Наличный платёж' WHEN 43 THEN 'Прочие финансовые операции' ELSE '' END AS TypeStr, " +
                        "Stage, InSum, OutSum, CurrencyRate, DateFact, DispatchComment, DispatchDate, Invoice2Number, " +
                        "'' AS Manager, CurrencyShortName " +
                        "FROM dbo.Doc WHERE PartnerID = " + partner_id.ToString() + " AND IsPosted = 1 " +
                        //"AND IsBalance = 1 " +
                        "AND IsInner = 0 AND [Type] NOT IN (50)" +
                        (showAll ? "" : date_clause) + " ORDER BY dbo.udfGetOutDocDate(Doc.ID) DESC, Doc.ID";
                    try
                    {
                        SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                        dataAdapter.Fill(dataSet, "Table");
                    }
                    catch { }

                    if (IsAudit())
                    {
                        try
                        {
                            using (SqlCommand sqlcmd = new SqlCommand())
                            {
                                sqlcmd.Connection = connTSC;
                                sqlcmd.CommandType = CommandType.Text;
                                sqlcmd.CommandTimeout = 7200;
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, GoodsRestID, Comment) VALUES (100, 'GetOrders', " +
                                    GetUserIDFromSession().ToString() + ", " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ", '" +
                                    showAll.ToString() + ";" + str_date1 + ";" + str_date2 + "')";
                                sqlcmd.ExecuteNonQuery();
                            }
                        }
                        catch { }
                    }


                    connTSC.Close();
                    connTSC.Dispose();
                    watch.Stop();
                    if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                        mLogger.Info($"{watch.ElapsedMilliseconds}");
                }
            }

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public int GetDaysOfReserve()
        {
            DateTime startTime = DateTime.Now;
            int result = 0;
            int partner_id = GetUserIDFromSession();

            if (partner_id != 0)
            {
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    var watch = Stopwatch.StartNew();
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText = "SELECT ISNULL((SELECT DaysOfReserve FROM dbo.[Partner] WHERE ID = " + partner_id.ToString() + "), 0)";
                        try
                        {
                            result = (short)sqlcmd.ExecuteScalar();
                        }
                        catch
                        {
                            result = 0;
                        }

                        if (IsAudit())
                        {
                            try
                            {
                                sqlcmd.CommandType = CommandType.Text;
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, DocStrID, NewOutQty) VALUES (100, 'GetDaysOfReserve', " +
                                    GetUserIDFromSession().ToString() + ", " + result.ToString().Replace(",", ".") + ", " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") + ")";
                                sqlcmd.ExecuteNonQuery();
                            }
                            catch { }
                        }
                    }
                    connTSC.Close();
                    connTSC.Dispose();

                    watch.Stop();
                    if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                        mLogger.Info($"{watch.ElapsedMilliseconds}");
                }
            }

            return result;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetBalans(DateTime dateBegin, DateTime dateEnd, bool showAll)
        {
            DataSet dataSet = new DataSet();

            //...

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet IsOrderingBlocked()
        {
            DataSet dataSet = new DataSet();
            int partnerId = GetUserIDFromSession();
            decimal partnerBalance = 0;

            if (partnerId != 0)
            {
                var isDurationOwerflowed = false;
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    var watch = Stopwatch.StartNew();
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandTimeout = 7200;

                        var useBalanceLimitation = false;
                        var useDurationLimitation = false;
                        var credit = 0m;
                        var balanceCache = 0m;
                        var currentBalance = 0m;
                        var notCachedBalance = 0m;
                        var limitDurationDays = 0;
                        sqlcmd.CommandText = "SELECT UseBalanceLimitation,UseLimitDuration,Credit,Balance," +
                            $"LimitDuration FROM Partner WHERE ID= { partnerId}";

                        using (var reader = sqlcmd.ExecuteReader())
                        {
                            reader.Read();
                            var obj = reader["UseBalanceLimitation"];
                            if (obj != null && obj != DBNull.Value)
                                useBalanceLimitation = Convert.ToBoolean(obj);

                            obj = reader["UseLimitDuration"];
                            if (obj != null && obj != DBNull.Value)
                                useDurationLimitation = Convert.ToBoolean(obj);

                            obj = reader["Credit"];
                            if (obj != null && obj != DBNull.Value)
                                credit = Convert.ToDecimal(obj);

                            obj = reader["Balance"];
                            if (obj != null && obj != DBNull.Value)
                                balanceCache = Convert.ToDecimal(obj);

                            obj = reader["LimitDuration"];
                            if (obj != null && obj != DBNull.Value)
                                limitDurationDays = Convert.ToInt32(obj);
                        }

                        sqlcmd.CommandText =
                            "SELECT CASE ISNULL(CurrencyShortName,'')" +
                            "WHEN 'USD' THEN(SELECT ROUND(ISNULL(SUM(ISNULL(DerivedInSum, 0) - ISNULL(DerivedOutSum, 0)), 0), 2)" +
                            "FROM ( SELECT CASE CurrencyShortName " +
                            "WHEN 'EUR' THEN ROUND(InSum * (SELECT ISNULL(SalesRate, 1.385) FROM dbo.Currency WHERE ShortName = 'USD'), 2) " +
                            "WHEN 'USD' THEN InSum " +
                            "WHEN 'руб' THEN ROUND(ROUND(InSum / CurrencyRate, 2) * (SELECT ISNULL(SalesRate, 1.385) FROM dbo.Currency WHERE ShortName = 'USD'), 2) " +
                            "END AS DerivedInSum, " +
                            "CASE CurrencyShortName " +
                            "WHEN 'EUR' THEN ROUND(OutSum* (SELECT ISNULL(SalesRate, 1.385) FROM dbo.Currency WHERE ShortName = 'USD'), 2) " +
                            "WHEN 'USD' THEN OutSum " +
                            "WHEN 'руб' THEN ROUND(ROUND(OutSum / CurrencyRate, 2) *(SELECT ISNULL(SalesRate, 1.385) FROM dbo.Currency WHERE ShortName = 'USD'), 2) " +
                            "END AS DerivedOutSum " +
                            "FROM ( " +
                            "SELECT DateFact = CASE WHEN[Type] IN(23, 26) THEN DispatchDate " +
                            "WHEN([Type] = 50) AND(MasterID IS NOT NULL AND MasterID > 0) THEN AssyDate " +
                            "ELSE DateFact END, CurrencyShortName, CurrencyRate, InSum, OutSum FROM dbo.Doc " +
                            "WHERE IsPosted = 1 AND IsBalance = 1 AND PartnerID = Partner.ID " +
                            ") AS t WHERE DateFact IS NOT NULL AND DateFact > LastBalanceUpdateTime) AS t2) " +
                            "WHEN 'руб' THEN(SELECT ROUND(ISNULL(SUM(ISNULL(DerivedInSum, 0) - ISNULL(DerivedOutSum, 0)), 0), 2) " +
                            "FROM ( SELECT CASE CurrencyShortName " +
                            "WHEN 'EUR' THEN ROUND(InSum * dbo.udfReturnCurrencyRate2(DateFact), 2) " +
                            "WHEN 'USD' THEN ROUND(ROUND(InSum / CurrencyRate, 2) * dbo.udfReturnCurrencyRate2(DateFact), 2) " +
                            "WHEN 'руб' THEN InSum END AS DerivedInSum, " +
                            "CASE CurrencyShortName " +
                            "WHEN 'EUR' THEN ROUND(OutSum * dbo.udfReturnCurrencyRate2(DateFact), 2) " +
                            "WHEN 'USD' THEN ROUND(ROUND(OutSum / CurrencyRate, 2) * dbo.udfReturnCurrencyRate2(DateFact), 2) " +
                            "WHEN 'руб' THEN OutSum " +
                            "END AS DerivedOutSum " +
                            "FROM (SELECT DateFact = CASE WHEN[Type] IN(23, 26) THEN DispatchDate " +
                            "WHEN([Type] = 50) AND(MasterID IS NOT NULL AND MasterID > 0) THEN AssyDate " +
                            "ELSE DateFact END, CurrencyShortName, CurrencyRate, InSum, OutSum FROM dbo.Doc " +
                            "WHERE IsPosted = 1 AND IsBalance = 1 AND PartnerID = Partner.ID " +
                            ") AS t WHERE DateFact IS NOT NULL AND DateFact > LastBalanceUpdateTime) AS t2) " +
                            "ELSE(SELECT ROUND(ISNULL(SUM(ISNULL(DerivedInSum, 0) - ISNULL(DerivedOutSum, 0)), 0), 2) " +
                            "FROM (SELECT CASE CurrencyShortName " +
                            "WHEN 'EUR' THEN InSum " +
                            "WHEN 'USD' THEN ROUND(InSum / CurrencyRate, 2) " +
                            "WHEN 'руб' THEN ROUND(InSum / CurrencyRate, 2) " +
                            "END AS DerivedInSum, " +
                            "CASE CurrencyShortName " +
                            "WHEN 'EUR' THEN OutSum " +
                            "WHEN 'USD' THEN ROUND(OutSum / CurrencyRate, 2) " +
                            "WHEN 'руб' THEN ROUND(OutSum / CurrencyRate, 2) " +
                            "END AS DerivedOutSum " +
                            "FROM (SELECT DateFact = CASE WHEN[Type] IN(23, 26) THEN DispatchDate " +
                            "WHEN([Type] = 50) AND(MasterID IS NOT NULL AND MasterID > 0) THEN AssyDate " +
                            "ELSE DateFact END, CurrencyShortName, CurrencyRate, InSum, OutSum FROM dbo.Doc " +
                            "WHERE IsPosted = 1 AND IsBalance = 1 AND PartnerID = Partner.ID " +
                            ") AS t WHERE DateFact IS NOT NULL AND DateFact > LastBalanceUpdateTime) AS t2 " +
                            $")	END FROM Partner WHERE ID= { partnerId}";
                        currentBalance = balanceCache + Convert.ToDecimal(sqlcmd.ExecuteScalar());

                        if (!useBalanceLimitation)
                            partnerBalance = 0;
                        else
                        {
                            sqlcmd.CommandText = "SELECT CASE ISNULL(CurrencyShortName, '')" +
                                "WHEN 'USD' THEN(SELECT ROUND(ISNULL(SUM(ISNULL(DerivedInSum, 0) - ISNULL(DerivedOutSum, 0)), 0), 2)" +
                                "FROM ( SELECT CASE CurrencyShortName " +
                                "WHEN 'EUR' THEN ROUND(InSum * (SELECT ISNULL(SalesRate, 1.385) FROM dbo.Currency WHERE ShortName = 'USD'), 2) " +
                                "WHEN 'USD' THEN InSum " +
                                "WHEN 'руб' THEN ROUND(ROUND(InSum / CurrencyRate, 2) * (SELECT ISNULL(SalesRate, 1.385) FROM dbo.Currency WHERE ShortName = 'USD'), 2) " +
                                "END AS DerivedInSum, " +
                               "CASE CurrencyShortName " +
                                "WHEN 'EUR' THEN ROUND(OutSum* (SELECT ISNULL(SalesRate, 1.385) FROM dbo.Currency WHERE ShortName = 'USD'), 2) " +
                                "WHEN 'USD' THEN OutSum " +
                                "WHEN 'руб' THEN ROUND(ROUND(OutSum / CurrencyRate, 2) *(SELECT ISNULL(SalesRate, 1.385) FROM dbo.Currency WHERE ShortName = 'USD'), 2) " +
                                "END AS DerivedOutSum " +
                                "FROM ( " +
                                "SELECT DateFact = CASE WHEN[Type] IN(23, 26) AND Stage NOT IN " +
                                "('начать сборку','сборка Кгд','сборка Мск','собран','передан на отправку') THEN DispatchDate " +
                                "WHEN[Type] IN(23, 26) AND Stage IN('начать сборку', 'сборка Кгд', 'сборка Мск', 'собран', " +
                                "'передан на отправку') THEN DateFact WHEN([Type] = 50) AND(MasterID IS NOT NULL AND MasterID > 0) " +
                                "THEN AssyDate ELSE DateFact END, CurrencyShortName, CurrencyRate, InSum, OutSum FROM dbo.Doc " +
                                "WHERE((IsPosted = 0 AND IsBalance = 0) OR(IsPosted = 1 AND IsBalance = 0) OR " +
                                "(IsPosted = 0 AND IsBalance = 1)) AND PartnerID = Partner.ID " +
                                " AND Stage IN('начать сборку', 'сборка Кгд', 'сборка Мск', 'собран', 'передан на отправку', " +
                                "'в пункте выдачи', 'на отправке', 'отправлен клиенту', 'получен клиентом','расформировать')" +
                                ") AS t WHERE DateFact IS NOT NULL AND DateFact > LastBalanceUpdateTime) AS t2) " +
                                "WHEN 'руб' THEN(SELECT ROUND(ISNULL(SUM(ISNULL(DerivedInSum, 0) - ISNULL(DerivedOutSum, 0)), 0), 2) " +
                                "FROM ( SELECT CASE CurrencyShortName " +
                                "WHEN 'EUR' THEN ROUND(InSum * dbo.udfReturnCurrencyRate2(DateFact), 2) " +
                                "WHEN 'USD' THEN ROUND(ROUND(InSum / CurrencyRate, 2) * dbo.udfReturnCurrencyRate2(DateFact), 2) " +
                                "WHEN 'руб' THEN InSum END AS DerivedInSum, " +
                                "CASE CurrencyShortName " +
                                "WHEN 'EUR' THEN ROUND(OutSum * dbo.udfReturnCurrencyRate2(DateFact), 2) " +
                                "WHEN 'USD' THEN ROUND(ROUND(OutSum / CurrencyRate, 2) * dbo.udfReturnCurrencyRate2(DateFact), 2) " +
                                "WHEN 'руб' THEN OutSum " +
                                "END AS DerivedOutSum " +
                                "FROM (SELECT DateFact = CASE WHEN[Type] IN(23, 26) AND Stage NOT IN " +
                                "('начать сборку','сборка Кгд','сборка Мск','собран','передан на отправку') THEN DispatchDate " +
                                "WHEN[Type] IN(23, 26) AND Stage IN('начать сборку', 'сборка Кгд', 'сборка Мск', 'собран', " +
                                "'передан на отправку') THEN DateFact WHEN([Type] = 50) AND(MasterID IS NOT NULL AND MasterID > 0) " +
                                "THEN AssyDate ELSE DateFact END, CurrencyShortName, CurrencyRate, InSum, OutSum FROM dbo.Doc " +
                                "WHERE((IsPosted = 0 AND IsBalance = 0) OR(IsPosted = 1 AND IsBalance = 0) OR " +
                                "(IsPosted = 0 AND IsBalance = 1)) AND PartnerID = Partner.ID " +
                                " AND Stage IN('начать сборку', 'сборка Кгд', 'сборка Мск', 'собран', 'передан на отправку', " +
                                "'в пункте выдачи', 'на отправке', 'отправлен клиенту', 'получен клиентом','расформировать')" +
                                ") AS t WHERE DateFact IS NOT NULL AND DateFact> LastBalanceUpdateTime) AS t2) " +
                                "ELSE(SELECT ROUND(ISNULL(SUM(ISNULL(DerivedInSum, 0) - ISNULL(DerivedOutSum, 0)), 0), 2) " +
                                "FROM (SELECT CASE CurrencyShortName " +
                                "WHEN 'EUR' THEN InSum " +
                                "WHEN 'USD' THEN ROUND(InSum / CurrencyRate, 2) " +
                                "WHEN 'руб' THEN ROUND(InSum / CurrencyRate, 2) " +
                                "END AS DerivedInSum, " +
                                "CASE CurrencyShortName " +
                                "WHEN 'EUR' THEN OutSum " +
                                "WHEN 'USD' THEN ROUND(OutSum / CurrencyRate, 2) " +
                                "WHEN 'руб' THEN ROUND(OutSum / CurrencyRate, 2) " +
                                "END AS DerivedOutSum " +
                                "FROM (SELECT DateFact = CASE WHEN[Type] IN(23, 26) AND Stage NOT IN " +
                                "('начать сборку','сборка Кгд','сборка Мск','собран','передан на отправку') THEN DispatchDate " +
                                "WHEN[Type] IN(23, 26) AND Stage IN('начать сборку', 'сборка Кгд', 'сборка Мск', 'собран', " +
                                "'передан на отправку') THEN DateFact WHEN([Type] = 50) AND(MasterID IS NOT NULL AND MasterID > 0) " +
                                "THEN AssyDate ELSE DateFact END, CurrencyShortName, CurrencyRate, InSum, OutSum FROM dbo.Doc " +
                                "WHERE((IsPosted = 0 AND IsBalance = 0) OR(IsPosted = 1 AND IsBalance = 0) OR " +
                                "(IsPosted = 0 AND IsBalance = 1)) AND PartnerID = Partner.ID " +
                                " AND Stage IN('начать сборку', 'сборка Кгд', 'сборка Мск', 'собран', 'передан на отправку', " +
                                "'в пункте выдачи', 'на отправке', 'отправлен клиенту', 'получен клиентом','расформировать')" +
                                ") AS t WHERE DateFact IS NOT NULL AND DateFact > LastBalanceUpdateTime) AS t2 " +
                                $")	END FROM Partner WHERE ID= { partnerId}";
                            notCachedBalance = Convert.ToDecimal(sqlcmd.ExecuteScalar());
                            partnerBalance = credit + currentBalance + notCachedBalance;
                        }

                        if (useDurationLimitation)
                        {
                            var limitDate = DateTime.Today.AddDays(-limitDurationDays);
                            sqlcmd.CommandText = "SELECT CASE ISNULL(CurrencyShortName,'')" +
                           "WHEN 'USD' THEN(SELECT ROUND(ISNULL(SUM(ISNULL(-DerivedOutSum, 0)), 0), 2)" +
                           "FROM ( SELECT CASE CurrencyShortName " +
                           "WHEN 'EUR' THEN ROUND(InSum * (SELECT ISNULL(SalesRate, 1.385) FROM dbo.Currency WHERE ShortName = 'USD'), 2) " +
                           "WHEN 'USD' THEN InSum " +
                           "WHEN 'руб' THEN ROUND(ROUND(InSum / CurrencyRate, 2) * (SELECT ISNULL(SalesRate, 1.385) FROM dbo.Currency WHERE ShortName = 'USD'), 2) " +
                           "END AS DerivedInSum, " +
                           "CASE CurrencyShortName " +
                           "WHEN 'EUR' THEN ROUND(OutSum* (SELECT ISNULL(SalesRate, 1.385) FROM dbo.Currency WHERE ShortName = 'USD'), 2) " +
                           "WHEN 'USD' THEN OutSum " +
                           "WHEN 'руб' THEN ROUND(ROUND(OutSum / CurrencyRate, 2) *(SELECT ISNULL(SalesRate, 1.385) FROM dbo.Currency WHERE ShortName = 'USD'), 2) " +
                           "END AS DerivedOutSum " +
                           "FROM ( " +
                           "SELECT DateFact = CASE WHEN[Type] IN(23, 26) THEN DispatchDate " +
                           "WHEN([Type] = 50) AND(MasterID IS NOT NULL AND MasterID > 0) THEN AssyDate " +
                           "ELSE DateFact END, CurrencyShortName, CurrencyRate, InSum, OutSum FROM dbo.Doc " +
                           "WHERE IsPosted = 1 AND IsBalance = 1 AND PartnerID = Partner.ID " +
                           ") AS t WHERE DateFact IS NOT NULL AND DateFact >= @limitationDate) AS t2) " +
                           "WHEN 'руб' THEN(SELECT ROUND(ISNULL(SUM(ISNULL(-DerivedOutSum, 0)), 0), 2) " +
                           "FROM ( SELECT CASE CurrencyShortName " +
                           "WHEN 'EUR' THEN ROUND(InSum * dbo.udfReturnCurrencyRate2(DateFact), 2) " +
                           "WHEN 'USD' THEN ROUND(ROUND(InSum / CurrencyRate, 2) * dbo.udfReturnCurrencyRate2(DateFact), 2) " +
                           "WHEN 'руб' THEN InSum END AS DerivedInSum, " +
                           "CASE CurrencyShortName " +
                           "WHEN 'EUR' THEN ROUND(OutSum * dbo.udfReturnCurrencyRate2(DateFact), 2) " +
                           "WHEN 'USD' THEN ROUND(ROUND(OutSum / CurrencyRate, 2) * dbo.udfReturnCurrencyRate2(DateFact), 2) " +
                           "WHEN 'руб' THEN OutSum " +
                           "END AS DerivedOutSum " +
                           "FROM (SELECT DateFact = CASE WHEN[Type] IN(23, 26) THEN DispatchDate " +
                           "WHEN([Type] = 50) AND(MasterID IS NOT NULL AND MasterID > 0) THEN AssyDate " +
                           "ELSE DateFact END, CurrencyShortName, CurrencyRate, InSum, OutSum FROM dbo.Doc " +
                           "WHERE IsPosted = 1 AND IsBalance = 1 AND PartnerID = Partner.ID " +
                           ") AS t WHERE DateFact IS NOT NULL AND DateFact >= @limitationDate) AS t2) " +
                           "ELSE(SELECT ROUND(ISNULL(SUM(ISNULL(-DerivedOutSum, 0)), 0), 2) " +
                           "FROM (SELECT CASE CurrencyShortName " +
                                       "WHEN 'EUR' THEN InSum " +
                                       "WHEN 'USD' THEN ROUND(InSum / CurrencyRate, 2) " +
                                       "WHEN 'руб' THEN ROUND(InSum / CurrencyRate, 2) " +
                                   "END AS DerivedInSum, " +
                                   "CASE CurrencyShortName " +
                                       "WHEN 'EUR' THEN OutSum " +
                                       "WHEN 'USD' THEN ROUND(OutSum / CurrencyRate, 2) " +
                                       "WHEN 'руб' THEN ROUND(OutSum / CurrencyRate, 2) " +
                                   "END AS DerivedOutSum " +
                                   "FROM (SELECT DateFact = CASE WHEN[Type] IN(23, 26) THEN DispatchDate " +
                                   "WHEN([Type] = 50) AND(MasterID IS NOT NULL AND MasterID > 0) THEN AssyDate " +
                                   "ELSE DateFact END, CurrencyShortName, CurrencyRate, InSum, OutSum FROM dbo.Doc " +
                                   "WHERE IsPosted = 1 AND IsBalance = 1 AND PartnerID = Partner.ID " +
                                   ") AS t WHERE DateFact IS NOT NULL AND DateFact >= @limitationDate) AS t2 " +
                            $")	END FROM Partner WHERE ID={partnerId}";
                            sqlcmd.Parameters.Add(new SqlParameter
                            {
                                ParameterName = "@limitationDate",
                                DbType = DbType.DateTime,
                                Value = limitDate
                            });
                            var durationLimitationOutcome = Convert.ToDecimal(sqlcmd.ExecuteScalar());
                            isDurationOwerflowed = currentBalance < 0 &&
                                (durationLimitationOutcome > currentBalance);
                        }
                    }
                    connTSC.Close();
                    connTSC.Dispose();
                    watch.Stop();
                }

                var resTable = new DataTable("Table");
                resTable.Columns.Add(new DataColumn("IsCreditOverflow", typeof(bool)));
                resTable.Columns.Add(new DataColumn("IsDurationOverflow", typeof(bool)));
                dataSet.Tables.Add(resTable);
                var row = resTable.NewRow();

                row["IsCreditOverflow"] = partnerBalance < 0;
                row["IsDurationOverflow"] = isDurationOwerflowed;
                resTable.Rows.Add(row);
            }
            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public decimal CalcPartnerBalance()
        {
            DateTime startTime = DateTime.Now;
            decimal result = 0;
            int _partner_id = GetUserIDFromSession();

            if (_partner_id != 0)
            {
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    var watch = Stopwatch.StartNew();
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText =
                            "SELECT Balance + CASE ISNULL(CurrencyShortName,'')" +
                            "WHEN 'USD' THEN(SELECT ROUND(ISNULL(SUM(ISNULL(DerivedInSum, 0) - ISNULL(DerivedOutSum, 0)), 0), 2)" +
                            "FROM ( SELECT CASE CurrencyShortName " +
                            "WHEN 'EUR' THEN ROUND(InSum * (SELECT ISNULL(SalesRate, 1.385) FROM dbo.Currency WHERE ShortName = 'USD'), 2) " +
                            "WHEN 'USD' THEN InSum " +
                            "WHEN 'руб' THEN ROUND(ROUND(InSum / CurrencyRate, 2) * (SELECT ISNULL(SalesRate, 1.385) FROM dbo.Currency WHERE ShortName = 'USD'), 2) " +
                            "END AS DerivedInSum, " +
                            "CASE CurrencyShortName " +
                            "WHEN 'EUR' THEN ROUND(OutSum* (SELECT ISNULL(SalesRate, 1.385) FROM dbo.Currency WHERE ShortName = 'USD'), 2) " +
                            "WHEN 'USD' THEN OutSum " +
                            "WHEN 'руб' THEN ROUND(ROUND(OutSum / CurrencyRate, 2) *(SELECT ISNULL(SalesRate, 1.385) FROM dbo.Currency WHERE ShortName = 'USD'), 2) " +
                            "END AS DerivedOutSum " +
                            "FROM ( " +
                            "SELECT DateFact = CASE WHEN[Type] IN(23, 26) THEN DispatchDate " +
                            "WHEN([Type] = 50) AND(MasterID IS NOT NULL AND MasterID > 0) THEN AssyDate " +
                            "ELSE DateFact END, CurrencyShortName, CurrencyRate, InSum, OutSum FROM dbo.Doc " +
                            "WHERE IsPosted = 1 AND IsBalance = 1 AND PartnerID = Partner.ID " +
                            ") AS t WHERE DateFact IS NOT NULL AND DateFact > LastBalanceUpdateTime) AS t2) " +
                            "WHEN 'руб' THEN(SELECT ROUND(ISNULL(SUM(ISNULL(DerivedInSum, 0) - ISNULL(DerivedOutSum, 0)), 0), 2) " +
                            "FROM ( SELECT CASE CurrencyShortName " +
                            "WHEN 'EUR' THEN ROUND(InSum * dbo.udfReturnCurrencyRate2(DateFact), 2) " +
                            "WHEN 'USD' THEN ROUND(ROUND(InSum / CurrencyRate, 2) * dbo.udfReturnCurrencyRate2(DateFact), 2) " +
                            "WHEN 'руб' THEN InSum END AS DerivedInSum, " +
                            "CASE CurrencyShortName " +
                            "WHEN 'EUR' THEN ROUND(OutSum * dbo.udfReturnCurrencyRate2(DateFact), 2) " +
                            "WHEN 'USD' THEN ROUND(ROUND(OutSum / CurrencyRate, 2) * dbo.udfReturnCurrencyRate2(DateFact), 2) " +
                            "WHEN 'руб' THEN OutSum " +
                            "END AS DerivedOutSum " +
                            "FROM (SELECT DateFact = CASE WHEN[Type] IN(23, 26) THEN DispatchDate " +
                            "WHEN([Type] = 50) AND(MasterID IS NOT NULL AND MasterID > 0) THEN AssyDate " +
                            "ELSE DateFact END, CurrencyShortName, CurrencyRate, InSum, OutSum FROM dbo.Doc " +
                            "WHERE IsPosted = 1 AND IsBalance = 1 AND PartnerID = Partner.ID " +
                            ") AS t WHERE DateFact IS NOT NULL AND DateFact > LastBalanceUpdateTime) AS t2) " +
                            "ELSE(SELECT ROUND(ISNULL(SUM(ISNULL(DerivedInSum, 0) - ISNULL(DerivedOutSum, 0)), 0), 2) " +
                            "FROM (SELECT CASE CurrencyShortName " +
                            "WHEN 'EUR' THEN InSum " +
                            "WHEN 'USD' THEN ROUND(InSum / CurrencyRate, 2) " +
                            "WHEN 'руб' THEN ROUND(InSum / CurrencyRate, 2) " +
                            "END AS DerivedInSum, " +
                            "CASE CurrencyShortName " +
                            "WHEN 'EUR' THEN OutSum " +
                            "WHEN 'USD' THEN ROUND(OutSum / CurrencyRate, 2) " +
                            "WHEN 'руб' THEN ROUND(OutSum / CurrencyRate, 2) " +
                            "END AS DerivedOutSum " +
                            "FROM (SELECT DateFact = CASE WHEN[Type] IN(23, 26) THEN DispatchDate " +
                            "WHEN([Type] = 50) AND(MasterID IS NOT NULL AND MasterID > 0) THEN AssyDate " +
                            "ELSE DateFact END, CurrencyShortName, CurrencyRate, InSum, OutSum FROM dbo.Doc " +
                            "WHERE IsPosted = 1 AND IsBalance = 1 AND PartnerID = Partner.ID " +
                            ") AS t WHERE DateFact IS NOT NULL AND DateFact > LastBalanceUpdateTime) AS t2 " +
                            $")	END FROM Partner WHERE ID= { _partner_id}";

                        try
                        {
                            result = Convert.ToDecimal(sqlcmd.ExecuteScalar());

                        }
                        catch
                        {
                            result = 0;
                        }

                        if (IsAudit())
                        {
                            try
                            {
                                sqlcmd.CommandType = CommandType.Text;
                                sqlcmd.Parameters.Clear();
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, NewInPrice) VALUES (100, 'CalcPartnerBalance', " +
                                    GetUserIDFromSession().ToString() + ", " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    ", " + result.ToString().Replace(",", ".") + ")";
                                sqlcmd.ExecuteNonQuery();
                            }
                            catch { }
                        }
                    }
                    connTSC.Close();
                    connTSC.Dispose();
                    watch.Stop();
                    if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                        mLogger.Info($"{watch.ElapsedMilliseconds}");
                }
            }

            return result;


            //DateTime startTime = DateTime.Now;
            //decimal result = 0;
            //int _partner_id = GetUserIDFromSession();

            //if (_partner_id != 0)
            //{
            //    SqlConnection connTSC = GetDBConnection();
            //    if (connTSC != null && connTSC.State == ConnectionState.Open)
            //    {
            //        var watch = Stopwatch.StartNew();
            //        using (SqlCommand sqlcmd = new SqlCommand())
            //        {
            //            sqlcmd.Connection = connTSC;
            //            sqlcmd.CommandType = CommandType.StoredProcedure;
            //            sqlcmd.CommandTimeout = 7200;
            //            sqlcmd.CommandText = "spCalcPartnerBalance";
            //            sqlcmd.Parameters.Clear();
            //            //
            //            SqlParameter partner_id = new SqlParameter();
            //            partner_id.Direction = ParameterDirection.Input;
            //            partner_id.ParameterName = "@partner_id";
            //            partner_id.SqlDbType = SqlDbType.Int;
            //            partner_id.Value = _partner_id;
            //            sqlcmd.Parameters.Add(partner_id);
            //            //
            //            SqlParameter balance = new SqlParameter();
            //            balance.Direction = ParameterDirection.Output;
            //            balance.ParameterName = "@balance";
            //            balance.SqlDbType = SqlDbType.Float;
            //            sqlcmd.Parameters.Add(balance);
            //            //
            //            SqlParameter currency_short_name = new SqlParameter();
            //            currency_short_name.Direction = ParameterDirection.Output;
            //            currency_short_name.ParameterName = "@currency_short_name";
            //            currency_short_name.SqlDbType = SqlDbType.VarChar;
            //            currency_short_name.Size = 3;
            //            sqlcmd.Parameters.Add(currency_short_name);
            //            //
            //            try
            //            {
            //                sqlcmd.ExecuteNonQuery();
            //                if (balance.Value != null)
            //                {
            //                    result = Convert.ToDecimal((double)balance.Value);
            //                }
            //            }
            //            catch
            //            {
            //                result = 0;
            //            }

            //            if (IsAudit())
            //            {
            //                try
            //                {
            //                    sqlcmd.CommandType = CommandType.Text;
            //                    sqlcmd.Parameters.Clear();
            //                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, NewInPrice) VALUES (100, 'CalcPartnerBalance', " +
            //                        GetUserIDFromSession().ToString() + ", " +
            //                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
            //                        ", " + result.ToString().Replace(",", ".") + ")";
            //                    sqlcmd.ExecuteNonQuery();
            //                }
            //                catch { }
            //            }
            //        }
            //        connTSC.Close();
            //        connTSC.Dispose();
            //        watch.Stop();
            //        if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
            //            mLogger.Info($"{watch.ElapsedMilliseconds}");
            //    }
            //}

            //return result;
        }

        [WebMethod(EnableSession = true)]
        public decimal RestCredit()
        {
            decimal result = 0;

            //...

            return result;
        }

        [WebMethod(EnableSession = true)]
        public int RestDaysOfReserve()
        {
            int result = 0;

            //...

            return result;
        }

        [WebMethod(EnableSession = true)]
        public object Test(object obj)
        {
            object result = new object();
            //...
            return result;
        }

        [WebMethod(EnableSession = true)]
        public DataSet Temp(object obj)
        {
            DataSet dataSet = new DataSet();

            //...

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public string Ping()
        {
            return "ok!";
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetMakerList()
        {
            DateTime startTime = DateTime.Now;
            string query, externalUrl;
            DataSet dataSet = new DataSet();
            var watch = Stopwatch.StartNew();
            if (GetUserIDFromSession() != 0)
            {
                try
                {
                    externalUrl = Session["externalUrl"].ToString();
                }
                catch
                {
                    externalUrl = "";
                }
                if (!string.IsNullOrEmpty(externalUrl))
                {
                    try
                    {
                        using (ExternalTSWS.Service extSvc = new ExternalTSWS.Service() { Url = externalUrl })
                        {
                            dataSet = extSvc.GetMakerListForExternalCall(
                                (Session["externalLogin"] ?? "").ToString(),
                                (Session["externalPassword"] ?? "").ToString());
                        }
                    }
                    catch (Exception ex) { }
                }
                else
                {
                    SqlConnection connTSC = GetDBConnection();
                    if (connTSC != null && connTSC.State == ConnectionState.Open)
                    {
                        query = "SELECT DISTINCT Maker FROM dbo.Car WHERE Maker IS NOT NULL AND LEN(Maker) > 0 ORDER BY Maker";
                        try
                        {
                            SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                            dataAdapter.Fill(dataSet, "Table");
                        }
                        catch { }

                        if (IsAudit())
                        {
                            try
                            {
                                using (SqlCommand sqlcmd = new SqlCommand())
                                {
                                    sqlcmd.Connection = connTSC;
                                    sqlcmd.CommandType = CommandType.Text;
                                    sqlcmd.CommandTimeout = 7200;
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, GoodsRestID) VALUES (100, 'GetMakerList', " +
                                        GetUserIDFromSession().ToString() + ", " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                    sqlcmd.ExecuteNonQuery();
                                }
                            }
                            catch { }
                        }

                        connTSC.Close();
                        connTSC.Dispose();
                    }
                }
            }
            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");
            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetModelList(string maker)
        {
            DateTime startTime = DateTime.Now;
            string query, externalUrl;
            DataSet dataSet = new DataSet();

            var watch = Stopwatch.StartNew();
            if (!String.IsNullOrEmpty(maker) && GetUserIDFromSession() != 0)
            {
                try
                {
                    externalUrl = Session["externalUrl"].ToString();
                }
                catch
                {
                    externalUrl = "";
                }
                if (!string.IsNullOrEmpty(externalUrl))
                {
                    try
                    {
                        using (ExternalTSWS.Service extSvc = new ExternalTSWS.Service() { Url = externalUrl })
                        {
                            dataSet = extSvc.GetModelListForExternalCall(maker,
                                (Session["externalLogin"] ?? "").ToString(), (Session["externalPassword"] ?? "").ToString());
                        }
                    }
                    catch (Exception ex) { }
                }
                else
                {
                    SqlConnection connTSC = GetDBConnection();
                    if (connTSC != null && connTSC.State == ConnectionState.Open)
                    {
                        query = "SELECT DISTINCT Model FROM dbo.Car WHERE (Model IS NOT NULL AND LEN(Model) > 0) AND Maker = '" +
                            maker + "' ORDER BY Model";
                        try
                        {
                            SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                            dataAdapter.Fill(dataSet, "Table");
                        }
                        catch { }

                        if (IsAudit())
                        {
                            try
                            {
                                using (SqlCommand sqlcmd = new SqlCommand())
                                {
                                    sqlcmd.Connection = connTSC;
                                    sqlcmd.CommandType = CommandType.Text;
                                    sqlcmd.CommandTimeout = 7200;
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, GoodsRestID, Comment) VALUES (100, 'GetModelList', " +
                                        GetUserIDFromSession().ToString() + ", " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ", '" + maker + "')";
                                    sqlcmd.ExecuteNonQuery();
                                }
                            }
                            catch { }
                        }

                        connTSC.Close();
                        connTSC.Dispose();
                    }
                }
            }
            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");
            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetEngineList(string maker, string model)
        {
            DateTime startTime = DateTime.Now;
            string query, externalUrl;
            DataSet dataSet = new DataSet();
            var watch = Stopwatch.StartNew();

            if ((!String.IsNullOrEmpty(maker) || !String.IsNullOrEmpty(model)) && GetUserIDFromSession() != 0)
            {
                try
                {
                    externalUrl = Session["externalUrl"].ToString();
                }
                catch
                {
                    externalUrl = "";
                }
                if (!string.IsNullOrEmpty(externalUrl))
                {
                    try
                    {
                        using (ExternalTSWS.Service extSvc = new ExternalTSWS.Service() { Url = externalUrl })
                        {
                            dataSet = extSvc.GetEngineListForExternalCall(maker, model,
                                (Session["externalLogin"] ?? "").ToString(),
                                (Session["externalPassword"] ?? "").ToString());
                        }
                    }
                    catch (Exception ex) { }
                }
                else
                {
                    SqlConnection connTSC = GetDBConnection();
                    if (connTSC != null && connTSC.State == ConnectionState.Open)
                    {
                        query = "SELECT DISTINCT Engine FROM dbo.Car WHERE (Engine IS NOT NULL AND LEN(Engine) > 0) " +
                            (maker.Length > 0 ? "AND Maker = '" + maker + "'" : "") + (model.Length > 0 ? "AND Model = '" + model + "'" : "") +
                            "ORDER BY Engine";
                        try
                        {
                            SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                            dataAdapter.Fill(dataSet, "Table");
                        }
                        catch { }

                        if (IsAudit())
                        {
                            try
                            {
                                using (SqlCommand sqlcmd = new SqlCommand())
                                {
                                    sqlcmd.Connection = connTSC;
                                    sqlcmd.CommandType = CommandType.Text;
                                    sqlcmd.CommandTimeout = 7200;
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, GoodsRestID, Comment) VALUES (100, 'GetEngineList', " +
                                        GetUserIDFromSession().ToString() + ", " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ", '" + maker + ";" +
                                        (model.Length > 47 ? model.Substring(0, 47) : model) + "')";
                                    sqlcmd.ExecuteNonQuery();
                                }
                            }
                            catch { }
                        }
                        connTSC.Close();
                        connTSC.Dispose();
                    }
                }
            }
            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetApplication(string maker, string model, string engine)
        {
            DateTime startTime = DateTime.Now;
            string query, s, externalUrl;
            DataSet dataSet = new DataSet();
            var watch = Stopwatch.StartNew();

            if ((!String.IsNullOrEmpty(maker) || !String.IsNullOrEmpty(model) || !String.IsNullOrEmpty(engine)) && GetUserIDFromSession() != 0)
            {
                try
                {
                    externalUrl = Session["externalUrl"].ToString();
                }
                catch
                {
                    externalUrl = "";
                }
                if (!string.IsNullOrEmpty(externalUrl))
                {
                    try
                    {
                        using (ExternalTSWS.Service extSvc = new ExternalTSWS.Service() { Url = externalUrl })
                        {
                            dataSet = extSvc.GetApplicationForExternalCall(maker, model, engine,
                                (Session["externalLogin"] ?? "").ToString(), (Session["externalPassword"] ?? "").ToString());
                        }
                    }
                    catch (Exception ex) { }
                }
                else
                {
                    SqlConnection connTSC = GetDBConnection();
                    if (connTSC != null && connTSC.State == ConnectionState.Open)
                    {
                        query = "SELECT cars.Maker, cars.Model, cars.Engine, " +
                            //with nulls
                            "st.GoodsApplicationComment AS StarterComment, st.Number AS StarterNumber, " +
                            "alt.GoodsApplicationComment AS AlternatorComment, alt.Number AS AlternatorNumber, " +
                            //with spaces instead of nulls
                            //"ISNULL(st.GoodsApplicationComment, '') AS StarterComment, ISNULL(st.Number, '') AS StarterNumber, " +
                            //"ISNULL(alt.GoodsApplicationComment, '') AS AlternatorComment, ISNULL(alt.Number, '') AS AlternatorNumber, " +
                            //
                            "heaters.HeaterComment, heaters.HeaterNumber, " +
                            //
                            "cars.Years FROM (SELECT Car.ID, Car.Maker, Car.Model, Car.Engine, Car.YearFrom, Car.YearTo, " +
                            "CONVERT(VARCHAR(4), YEAR(YearFrom)) + '.' + REPLICATE('0', 2 - LEN(CONVERT(VARCHAR(2), MONTH(YearFrom)))) + CONVERT(VARCHAR(2), MONTH(YearFrom)) + '-' + " +
                            "ISNULL(CONVERT(VARCHAR(4), YEAR(YearTo)) + '.' + REPLICATE('0', 2 - LEN(CONVERT(VARCHAR(2), MONTH(YearTo)))) + CONVERT(VARCHAR(2), MONTH(YearTo)), '') AS Years " +
                            "FROM dbo.Car WHERE  Car.ID > 0" +
                            //prms:
                            (maker.Length > 0 ? " AND Car.Maker = '" + maker + "'" : "") + (model.Length > 0 ? " AND Car.Model = '" + model + "'" : "") +
                            (engine.Length > 0 ? " AND Car.Engine = '" + engine + "'" : "") +
                            //
                            ") AS cars LEFT OUTER JOIN " +
                            "(SELECT Car.ID,ISNULL((SELECT TOP 1 Number FROM dbo.[Cross] WHERE CrossGroupID = " +
                            "(SELECT TOP 1 CrossGroupID FROM dbo.[Cross] WHERE Number = GoodsApplication.Number) AND LEN(IsPrior) > 0 ORDER BY ID),'') AS Number, " +
                            "ISNULL(GoodsApplication.Comment, '') AS GoodsApplicationComment " +
                            "FROM dbo.Car LEFT OUTER JOIN  dbo.GoodsApplication ON GoodsApplication.CarID = Car.ID " +
                            "WHERE Car.ID > 0 " +
                            //prms:
                            (maker.Length > 0 ? "AND Car.Maker = '" + maker + "'" : "") + (model.Length > 0 ? "AND Car.Model = '" + model + "'" : "") +
                            (engine.Length > 0 ? "AND Car.Engine = '" + engine + "'" : "") +
                            //old code:
                            //"AND (SELECT TOP 1 GoodsGroupID FROM dbo.Goods WHERE Number = GoodsApplication.Number ORDER BY ID DESC) = 16 " +
                            //new code:
                            " AND 16 IN (SELECT ISNULL((SELECT DISTINCT TOP 1 GoodsGroupID FROM dbo.Goods " +
                            "WHERE Number IN (SELECT Number FROM dbo.[Cross] WHERE CrossGroupID IN (" +
                            "SELECT CrossGroupID FROM dbo.[Cross] WHERE Number = GoodsApplication.Number)) AND GoodsGroupID = 16), 0))" +
                            //
                            ") AS st ON st.ID = cars.ID LEFT OUTER JOIN (SELECT Car.ID, ISNULL((SELECT TOP 1 Number FROM dbo.[Cross] WHERE CrossGroupID = " +
                            "(SELECT TOP 1 CrossGroupID FROM dbo.[Cross] WHERE Number = GoodsApplication.Number) AND LEN(IsPrior) > 0 ORDER BY ID),'') AS Number, " +
                            "ISNULL(GoodsApplication.Comment, '') AS GoodsApplicationComment " +
                            "FROM dbo.Car LEFT OUTER JOIN  dbo.GoodsApplication ON GoodsApplication.CarID = Car.ID " +
                            "WHERE Car.ID > 0 " +
                            //prms:
                            (maker.Length > 0 ? "AND Car.Maker = '" + maker + "'" : "") + (model.Length > 0 ? "AND Car.Model = '" + model + "'" : "") +
                            (engine.Length > 0 ? "AND Car.Engine = '" + engine + "'" : "") +
                            //old code:
                            //"AND (SELECT TOP 1 GoodsGroupID FROM dbo.Goods WHERE Number = GoodsApplication.Number ORDER BY ID DESC) = 5 " +
                            //new code:
                            " AND 5 IN (SELECT ISNULL((SELECT DISTINCT TOP 1 GoodsGroupID FROM dbo.Goods " +
                            "WHERE Number IN (SELECT Number FROM dbo.[Cross] WHERE CrossGroupID IN (" +
                            "SELECT CrossGroupID FROM dbo.[Cross] WHERE Number = GoodsApplication.Number)) AND GoodsGroupID = 5), 0))" +
                            //new code (20.05.2015):
                            ") AS alt ON alt.ID = cars.ID " +
                            "LEFT OUTER JOIN (" +
                            "SELECT CarID, ISNULL(GoodsApplication.Comment, '') AS HeaterComment, ISNULL(GoodsApplication.Number, '') AS HeaterNumber " +
                            "FROM dbo.GoodsApplication WHERE [Description] LIKE '%отопит%') AS heaters " +
                            "ON cars.ID = heaters.CarID " +
                            //
                            "ORDER BY cars.Maker, cars.Model, cars.YearFrom, cars.YearTo";
                        try
                        {
                            SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                            dataAdapter.Fill(dataSet, "Table");
                        }
                        catch { }

                        if (IsAudit())
                        {
                            try
                            {
                                s = maker + ";" + model + ";" + engine;
                                using (SqlCommand sqlcmd = new SqlCommand())
                                {
                                    sqlcmd.Connection = connTSC;
                                    sqlcmd.CommandType = CommandType.Text;
                                    sqlcmd.CommandTimeout = 7200;
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, GoodsRestID, Comment) VALUES (100, 'GetApplication', " +
                                        GetUserIDFromSession().ToString() + ", " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ", '" +
                                        (s.Length > 100 ? s.Substring(0, 100) : s) + "')";
                                    sqlcmd.ExecuteNonQuery();
                                }
                            }
                            catch { }
                        }

                        connTSC.Close();
                        connTSC.Dispose();
                    }
                }
            }
            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet FindApplication(int crossGroupID)
        {
            DateTime startTime = DateTime.Now;
            string query, externalUrl;
            DataSet dataSet = new DataSet();
            var watch = Stopwatch.StartNew();

            if (GetAccessAttribute("ShowUnitApplication") && crossGroupID > 0 && GetUserIDFromSession() != 0)
            {
                try
                {
                    externalUrl = Session["externalUrl"].ToString();
                }
                catch
                {
                    externalUrl = "";
                }
                if (!string.IsNullOrEmpty(externalUrl))
                {
                    try
                    {
                        using (ExternalTSWS.Service extSvc = new ExternalTSWS.Service() { Url = externalUrl })
                        {
                            dataSet = extSvc.FindApplicationForExternalCall(crossGroupID,
                                (Session["externalLogin"] ?? "").ToString(), (Session["externalPassword"] ?? "").ToString());
                        }
                    }
                    catch (Exception ex) { }
                }
                else
                {
                    SqlConnection connTSC = GetDBConnection();
                    if (connTSC != null && connTSC.State == ConnectionState.Open)
                    {
                        query = "SELECT Maker, Model, Engine, Comment, " +
                            "CONVERT(VARCHAR(4), YEAR(YearFrom)) + '.' + REPLICATE('0', 2 - LEN(CONVERT(VARCHAR(2), MONTH(YearFrom)))) + CONVERT(VARCHAR(2), MONTH(YearFrom)) + '-' + " +
                            "ISNULL(CONVERT(VARCHAR(4), YEAR(YearTo)) + '.' + REPLICATE('0', 2 - LEN(CONVERT(VARCHAR(2), MONTH(YearTo)))) +CONVERT(VARCHAR(2), MONTH(YearTo)),'') AS Years " +
                            "FROM dbo.Car WHERE ID IN (SELECT CarID FROM dbo.GoodsApplication WHERE Number IN " +
                            "(SELECT DISTINCT Number FROM dbo.[Cross] WHERE CrossGroupID = " + crossGroupID.ToString() + "))" +
                            "ORDER BY Maker, Model, Engine, YearFrom, YearTo, Comment";
                        try
                        {
                            SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                            dataAdapter.Fill(dataSet, "Table");
                        }
                        catch { }

                        if (IsAudit())
                        {
                            try
                            {
                                using (SqlCommand sqlcmd = new SqlCommand())
                                {
                                    sqlcmd.Connection = connTSC;
                                    sqlcmd.CommandType = CommandType.Text;
                                    sqlcmd.CommandTimeout = 7200;
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, DocStrID, NewOutQty, GoodsRestID) VALUES (100, 'FindApplication', " +
                                        GetUserIDFromSession().ToString() + ", " + crossGroupID.ToString() + ", " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                    sqlcmd.ExecuteNonQuery();
                                }
                            }
                            catch { }
                        }

                        connTSC.Close();
                        connTSC.Dispose();
                    }
                }
            }
            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet ApplicationComponentByCrossGroupID(int crossGroupID)
        {
            DateTime startTime = DateTime.Now;
            string query, externalUrl;
            DataSet dataSet = new DataSet();
            var watch = Stopwatch.StartNew();

            if (GetAccessAttribute("ShowPartApplication") && crossGroupID > 0 && GetUserIDFromSession() != 0)
            {
                try
                {
                    externalUrl = Session["externalUrl"].ToString();
                }
                catch
                {
                    externalUrl = "";
                }
                if (!string.IsNullOrEmpty(externalUrl))
                {
                    try
                    {
                        using (ExternalTSWS.Service extSvc = new ExternalTSWS.Service() { Url = externalUrl })
                        {
                            dataSet = extSvc.ApplicationComponentByCrossGroupIDForExternalCall(crossGroupID,
                                (Session["externalLogin"] ?? "").ToString(), (Session["externalPassword"] ?? "").ToString());
                        }
                    }
                    catch (Exception ex) { }
                }
                else
                {
                    SqlConnection connTSC = GetDBConnection();
                    if (connTSC != null && connTSC.State == ConnectionState.Open)
                    {
                        query = "SELECT DISTINCT ISNULL(GoodsNumber, '') AS PriorNumber FROM dbo.GoodsComponents WHERE ComponentNumber IN " +
                            "(SELECT Number FROM dbo.[Cross] WHERE CrossGroupID = " + crossGroupID.ToString() + ") ORDER BY PriorNumber";
                        try
                        {
                            SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                            dataAdapter.Fill(dataSet, "Table");
                        }
                        catch { }

                        if (IsAudit())
                        {
                            try
                            {
                                using (SqlCommand sqlcmd = new SqlCommand())
                                {
                                    sqlcmd.Connection = connTSC;
                                    sqlcmd.CommandType = CommandType.Text;
                                    sqlcmd.CommandTimeout = 7200;
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, DocStrID, NewOutQty, GoodsRestID) VALUES (100, 'AppComponentByCrossGroupID', " +
                                        GetUserIDFromSession().ToString() + ", " + crossGroupID.ToString() + ", " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                    sqlcmd.ExecuteNonQuery();
                                }
                            }
                            catch { }
                        }

                        connTSC.Close();
                        connTSC.Dispose();
                    }
                }
            }

            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet FindComponents(string number)
        {
            DateTime startTime = DateTime.Now;
            string query, externalUrl;
            DataSet dataSet = new DataSet();
            var watch = Stopwatch.StartNew();

            if (GetAccessAttribute("ShowPartApplication") && !String.IsNullOrEmpty(number) && GetUserIDFromSession() != 0)
            {
                try
                {
                    externalUrl = Session["externalUrl"].ToString();
                }
                catch
                {
                    externalUrl = "";
                }
                if (!string.IsNullOrEmpty(externalUrl))
                {
                    try
                    {
                        using (ExternalTSWS.Service extSvc = new ExternalTSWS.Service() { Url = externalUrl })
                        {
                            dataSet = extSvc.FindComponentsForExternalCall(number,
                                (Session["externalLogin"] ?? "").ToString(), (Session["externalPassword"] ?? "").ToString());
                        }
                    }
                    catch (Exception ex) { }
                }
                else
                {
                    SqlConnection connTSC = GetDBConnection();
                    if (connTSC != null && connTSC.State == ConnectionState.Open)
                    {
                        query = "SELECT [Type], ComponentNumber, ISNULL((SELECT TOP 1 Number FROM dbo.[Cross] WHERE CrossGroupID = " +
                            "(SELECT TOP 1 CrossGroupID FROM dbo.[Cross] WHERE Number = GoodsComponents.ComponentNumber) " +
                            "AND LEN(IsPrior) > 0 ORDER BY ID),'') AS PriorNumber,Comment " +
                            "FROM dbo.GoodsComponents WHERE GoodsNumber = '" + number + "' ORDER BY Type, ComponentNumber";
                        try
                        {
                            SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                            dataAdapter.Fill(dataSet, "Table");
                        }
                        catch { }

                        if (IsAudit())
                        {
                            try
                            {
                                using (SqlCommand sqlcmd = new SqlCommand())
                                {
                                    sqlcmd.Connection = connTSC;
                                    sqlcmd.CommandType = CommandType.Text;
                                    sqlcmd.CommandTimeout = 7200;
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, Number, NewOutQty, GoodsRestID) VALUES (100, 'FindComponents', " +
                                        GetUserIDFromSession().ToString() + ", '" + (number.Length > 20 ? number.Substring(0, 20) : number) + "', " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                    sqlcmd.ExecuteNonQuery();
                                }
                            }
                            catch { }
                        }

                        connTSC.Close();
                        connTSC.Dispose();
                    }
                }
            }

            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetImages(int crossGroupID)
        {
            DateTime startTime = DateTime.Now;
            string query, externalUrl;
            DataSet dataSet = new DataSet();
            var watch = Stopwatch.StartNew();

            if (GetAccessAttribute("ShowItemPicture") && crossGroupID > 0 && GetUserIDFromSession() != 0)
            {
                try
                {
                    externalUrl = Session["externalUrl"].ToString();
                }
                catch
                {
                    externalUrl = "";
                }
                if (!string.IsNullOrEmpty(externalUrl))
                {
                    try
                    {
                        using (ExternalTSWS.Service extSvc = new ExternalTSWS.Service() { Url = externalUrl })
                        {
                            dataSet = extSvc.GetImagesForExternalCall(crossGroupID,
                                (Session["externalLogin"] ?? "").ToString(), (Session["externalPassword"] ?? "").ToString());
                        }
                    }
                    catch (Exception ex) { }
                }
                else
                {
                    SqlConnection connTSC = GetDBConnection();
                    if (connTSC != null && connTSC.State == ConnectionState.Open)
                    {
                        query = "SELECT ID, Name, [Image], " +
                            "(SELECT TOP 1 IsPrior FROM dbo.[Cross] WHERE dbo.[Cross].Number = " +
                            "dbo.GoodsImages.Number OR dbo.[Cross].SearchNumber = dbo.GoodsImages.Number AND " +
                            "CrossGroupID = " + crossGroupID.ToString() + ") AS IsPrior FROM dbo.GoodsImages WHERE Number IN " +
                            "(SELECT DISTINCT SearchNumber FROM dbo.[Cross] WHERE CrossGroupID = " + crossGroupID.ToString() +
                            ") OR Number IN (SELECT DISTINCT Number FROM dbo.[Cross] WHERE CrossGroupID = " + crossGroupID.ToString() + ") " +
                            "ORDER BY IsPrior DESC, Number, Name, ID";
                        try
                        {
                            SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                            dataAdapter.Fill(dataSet, "Table");
                        }
                        catch { }

                        if (IsAudit())
                        {
                            try
                            {
                                using (SqlCommand sqlcmd = new SqlCommand())
                                {
                                    sqlcmd.Connection = connTSC;
                                    sqlcmd.CommandType = CommandType.Text;
                                    sqlcmd.CommandTimeout = 7200;
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, DocStrID, NewOutQty, GoodsRestID) VALUES (100, 'GetImages', " +
                                        GetUserIDFromSession().ToString() + ", " + crossGroupID.ToString() + ", " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                    sqlcmd.ExecuteNonQuery();
                                }
                            }
                            catch { }
                        }

                        connTSC.Close();
                        connTSC.Dispose();
                    }
                }
            }

            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetImageByNumber(string number)
        {
            DateTime startTime = DateTime.Now;
            string query, externalUrl;
            DataSet dataSet = new DataSet();
            var watch = Stopwatch.StartNew();

            if (GetAccessAttribute("ShowItemPicture") && !String.IsNullOrEmpty(number) && GetUserIDFromSession() != 0)
            {
                try
                {
                    externalUrl = Session["externalUrl"].ToString();
                }
                catch
                {
                    externalUrl = "";
                }
                if (!string.IsNullOrEmpty(externalUrl))
                {
                    try
                    {
                        using (ExternalTSWS.Service extSvc = new ExternalTSWS.Service() { Url = externalUrl })
                        {
                            dataSet = extSvc.GetImageByNumberForExternalCall(number,
                                (Session["externalLogin"] ?? "").ToString(), (Session["externalPassword"] ?? "").ToString());
                        }
                    }
                    catch (Exception ex) { }
                }
                else
                {
                    SqlConnection connTSC = GetDBConnection();
                    if (connTSC != null && connTSC.State == ConnectionState.Open)
                    {
                        query = "SELECT [Image] FROM dbo.GoodsImages WHERE Number = '" + number + "'";
                        try
                        {
                            SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                            dataAdapter.Fill(dataSet, "Table");
                        }
                        catch { }

                        if (IsAudit())
                        {
                            try
                            {
                                using (SqlCommand sqlcmd = new SqlCommand())
                                {
                                    sqlcmd.Connection = connTSC;
                                    sqlcmd.CommandType = CommandType.Text;
                                    sqlcmd.CommandTimeout = 7200;
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, Number, NewOutQty, GoodsRestID) VALUES (100, 'GetImageByNumber', " +
                                        GetUserIDFromSession().ToString() + ", '" + (number.Length > 20 ? number.Substring(0, 20) : number) + "', " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                    sqlcmd.ExecuteNonQuery();
                                }
                            }
                            catch { }
                        }

                        connTSC.Close();
                        connTSC.Dispose();
                    }
                }
            }
            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public int GetImagesCount(int crossGroupID)
        {
            DateTime startTime = DateTime.Now;
            int result = 0;
            int partner_id = GetUserIDFromSession();
            string externalUrl;
            var watch = Stopwatch.StartNew();

            if (GetAccessAttribute("ShowItemPicture") && partner_id != 0)
            {
                try
                {
                    externalUrl = Session["externalUrl"].ToString();
                }
                catch
                {
                    externalUrl = "";
                }
                if (!string.IsNullOrEmpty(externalUrl))
                {
                    try
                    {
                        using (ExternalTSWS.Service extSvc = new ExternalTSWS.Service() { Url = externalUrl })
                        {
                            result = extSvc.GetImagesCountForExternalCall(crossGroupID,
                                (Session["externalLogin"] ?? "").ToString(), (Session["externalPassword"] ?? "").ToString());
                        }
                    }
                    catch (Exception ex) { }
                }
                else
                {
                    SqlConnection connTSC = GetDBConnection();
                    if (connTSC != null && connTSC.State == ConnectionState.Open)
                    {
                        using (SqlCommand sqlcmd = new SqlCommand())
                        {
                            sqlcmd.Connection = connTSC;
                            sqlcmd.CommandType = CommandType.Text;
                            sqlcmd.CommandTimeout = 7200;
                            sqlcmd.CommandText = "SELECT COUNT(*) FROM dbo.GoodsImages WHERE Number IN (SELECT DISTINCT SearchNumber FROM dbo.[Cross] " +
                                "WHERE CrossGroupID = " + crossGroupID.ToString() + ") OR Number IN (SELECT DISTINCT Number FROM " +
                                "dbo.[Cross] WHERE CrossGroupID = " + crossGroupID.ToString() + ")";
                            try
                            {
                                result = (int)sqlcmd.ExecuteScalar();
                            }
                            catch
                            {
                                result = 0;
                            }
                        }

                        if (IsAudit())
                        {
                            try
                            {
                                using (SqlCommand sqlcmd = new SqlCommand())
                                {
                                    sqlcmd.Connection = connTSC;
                                    sqlcmd.CommandType = CommandType.Text;
                                    sqlcmd.CommandTimeout = 7200;
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, DocStrID, NewOutQty, GoodsRestID) VALUES (100, 'GetImagesCount', " +
                                        GetUserIDFromSession().ToString() + ", " + crossGroupID.ToString() + ", " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", " + result.ToString() + ")";
                                    sqlcmd.ExecuteNonQuery();
                                }
                            }
                            catch { }
                        }

                        connTSC.Close();
                        connTSC.Dispose();
                    }
                }
            }

            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");

            return result;
        }

        [WebMethod(EnableSession = true)]
        public string GetCrossComment(int CrossGroupID)
        {
            DateTime startTime = DateTime.Now;
            string result = "", externalUrl;
            int partner_id = GetUserIDFromSession();

            if (partner_id != 0)
            {
                var watch = Stopwatch.StartNew();
                try
                {
                    externalUrl = Session["externalUrl"].ToString();
                }
                catch
                {
                    externalUrl = "";
                }
                if (!string.IsNullOrEmpty(externalUrl))
                {
                    try
                    {
                        using (ExternalTSWS.Service extSvc = new ExternalTSWS.Service() { Url = externalUrl })
                        {
                            result = extSvc.GetCrossCommentForExternalCall(CrossGroupID,
                                (Session["externalLogin"] ?? "").ToString(), (Session["externalPassword"] ?? "").ToString());
                        }
                    }
                    catch (Exception ex) { }
                }
                else
                {
                    SqlConnection connTSC = GetDBConnection();
                    if (connTSC != null && connTSC.State == ConnectionState.Open)
                    {
                        using (SqlCommand sqlcmd = new SqlCommand())
                        {
                            sqlcmd.Connection = connTSC;
                            sqlcmd.CommandType = CommandType.Text;
                            sqlcmd.CommandTimeout = 7200;
                            sqlcmd.CommandText = "SELECT ISNULL((SELECT [Value] FROM dbo.CrossComment WHERE CrossGroupID = " + CrossGroupID.ToString() + "), '')";
                            try
                            {
                                result = (string)sqlcmd.ExecuteScalar();
                            }
                            catch
                            {
                                result = "";
                            }
                            if (IsAudit())
                            {
                                try
                                {
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, DocStrID, NewOutQty, Comment) VALUES (100, 'GetCrossComment', " +
                                        GetUserIDFromSession().ToString() + ", " + CrossGroupID.ToString() + ", " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", '" + (result.Length > 100 ? result.Substring(0, 100) : result) + "')";
                                    sqlcmd.ExecuteNonQuery();
                                }
                                catch { }
                            }
                        }
                        connTSC.Close();
                        connTSC.Dispose();
                    }
                }
                watch.Stop();
                if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                    mLogger.Info($"{watch.ElapsedMilliseconds}");
            }

            return result;
        }

        [WebMethod(EnableSession = true)]
        public DataSet ApplicationComponent(string number)
        {
            DateTime startTime = DateTime.Now;
            string query, externalUrl;
            DataSet dataSet = new DataSet();

            if (GetAccessAttribute("ShowPartApplication") && !String.IsNullOrEmpty(number) && GetUserIDFromSession() != 0)
            {
                var watch = Stopwatch.StartNew();
                try
                {
                    externalUrl = Session["externalUrl"].ToString();
                }
                catch
                {
                    externalUrl = "";
                }
                if (!string.IsNullOrEmpty(externalUrl))
                {
                    try
                    {
                        using (ExternalTSWS.Service extSvc = new ExternalTSWS.Service() { Url = externalUrl })
                        {
                            dataSet = extSvc.ApplicationComponentForExternalCall(number,
                                (Session["externalLogin"] ?? "").ToString(), (Session["externalPassword"] ?? "").ToString());
                        }
                    }
                    catch (Exception ex) { }
                }
                else
                {
                    SqlConnection connTSC = GetDBConnection();
                    if (connTSC != null && connTSC.State == ConnectionState.Open)
                    {
                        query = "SELECT DISTINCT ISNULL(GoodsNumber,'') AS GoodsNumber FROM dbo.GoodsComponents WHERE ComponentNumber = '" +
                            number + "' ORDER BY GoodsNumber";
                        try
                        {
                            SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                            dataAdapter.Fill(dataSet, "Table");
                        }
                        catch { }

                        if (IsAudit())
                        {
                            try
                            {
                                using (SqlCommand sqlcmd = new SqlCommand())
                                {
                                    sqlcmd.Connection = connTSC;
                                    sqlcmd.CommandType = CommandType.Text;
                                    sqlcmd.CommandTimeout = 7200;
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, Number, NewOutQty, GoodsRestID) VALUES (100, 'ApplicationComponent', " +
                                        GetUserIDFromSession().ToString() + ", '" + (number.Length > 20 ? number.Substring(0, 20) : number) + "', " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                    sqlcmd.ExecuteNonQuery();
                                }
                            }
                            catch { }
                        }

                        connTSC.Close();
                        connTSC.Dispose();
                    }
                }
                watch.Stop();
                if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                    mLogger.Info($"{watch.ElapsedMilliseconds}");
            }

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public bool GetShowUnitApplicationEnabled()
        {
            bool result = false;
            int partner_id = GetUserIDFromSession();

            if (partner_id != 0)
            {
                result = GetAccessAttribute("ShowUnitApplication");
            }

            return result;
        }

        [WebMethod(EnableSession = true)]
        public bool UpdateCartComment(int warehouseGroupID, string comment)
        {
            DateTime startTime = DateTime.Now;
            bool result = false;
            int doc_id = 0, comment_id = 0, partner_id = GetUserIDFromSession();

            if (partner_id != 0)
            {
                var watch = Stopwatch.StartNew();
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText = "SELECT ISNULL((SELECT TOP 1 ID FROM dbo.Doc WHERE Type = 60 AND PartnerID = " +
                            partner_id.ToString() + " ORDER BY ID DESC), 0)";
                        try
                        {
                            doc_id = (int)sqlcmd.ExecuteScalar();
                        }
                        catch
                        {
                            doc_id = 0;
                        }
                        if (doc_id > 0)
                        {
                            comment = comment.Replace("'", "`");
                            sqlcmd.CommandText = "SELECT ISNULL((SELECT TOP 1 ID FROM dbo.CartComments WHERE DocID = " +
                                doc_id.ToString() + " AND WarehouseGroupID = " + warehouseGroupID.ToString() +
                                " ORDER BY ID DESC), 0)";
                            try
                            {
                                comment_id = (int)sqlcmd.ExecuteScalar();
                            }
                            catch
                            {
                                comment_id = 0;
                            }
                            if (comment_id == 0)
                            {
                                sqlcmd.CommandText = "INSERT INTO dbo.CartComments(DocID, WarehouseGroupID, Comment) VALUES (" +
                                    doc_id.ToString() + ", " + warehouseGroupID.ToString() + ", '" + comment + "')";
                            }
                            else
                            {
                                sqlcmd.CommandText = "UPDATE dbo.CartComments SET Comment = '" + comment + "' WHERE ID = " + comment_id.ToString();
                            }
                            try
                            {
                                sqlcmd.ExecuteNonQuery();
                                result = true;
                            }
                            catch
                            {
                                result = false;
                            }

                            if (IsAudit())
                            {
                                try
                                {
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, DocStrID, NewOutQty, Comment) VALUES (100, 'UpdateCartComment', " +
                                        GetUserIDFromSession().ToString() + ", " + warehouseGroupID.ToString() + ", " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", '" + result.ToString() + ";" + (comment.Length > 93 ? comment.Substring(0, 93) : comment) + "')";
                                    sqlcmd.ExecuteNonQuery();
                                }
                                catch { }
                            }
                        }
                    }
                    connTSC.Close();
                    connTSC.Dispose();
                }

                watch.Stop();
                if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                    mLogger.Info($"{watch.ElapsedMilliseconds}");
            }

            return result;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetCartComments(int warehouseGroupID)
        {
            DateTime startTime = DateTime.Now;
            string query;
            int partner_id = GetUserIDFromSession(), doc_id = 0;
            DataSet dataSet = new DataSet();

            if (GetAccessAttribute("ShowCart") && partner_id != 0)
            {
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    var watch = Stopwatch.StartNew();
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText =
                            "SELECT ISNULL((SELECT TOP 1 ID FROM dbo.Doc WHERE Type = 60 AND PartnerID = " +
                            partner_id.ToString() + " ORDER BY ID DESC), 0)";
                        try
                        {
                            doc_id = (int)sqlcmd.ExecuteScalar();
                        }
                        catch
                        {
                            doc_id = 0;
                        }
                    }
                    if (doc_id > 0)
                    {
                        if (warehouseGroupID > 0)
                        {
                            query = "SELECT TOP 1 WarehouseGroupID, ISNULL(Comment, '') AS Comment FROM dbo.CartComments WHERE DocID = " +
                                doc_id.ToString() + " AND WarehouseGroupID = " + warehouseGroupID.ToString() + " ORDER BY ID DESC";
                        }
                        else
                        {
                            query = "SELECT WarehouseGroupID, ISNULL(Comment, '') AS Comment FROM dbo.CartComments WHERE DocID = " +
                                doc_id.ToString() + " ORDER BY WarehouseGroupID";
                        }
                        try
                        {
                            SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                            dataAdapter.Fill(dataSet, "Table");
                        }
                        catch { }

                        if (IsAudit())
                        {
                            try
                            {
                                using (SqlCommand sqlcmd2 = new SqlCommand())
                                {
                                    sqlcmd2.Connection = connTSC;
                                    sqlcmd2.CommandType = CommandType.Text;
                                    sqlcmd2.CommandTimeout = 7200;
                                    sqlcmd2.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, DocStrID, NewOutQty, GoodsRestID) VALUES (100, 'GetCartComments', " +
                                        GetUserIDFromSession().ToString() + ", " + warehouseGroupID.ToString() + ", " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                    sqlcmd2.ExecuteNonQuery();
                                }
                            }
                            catch { }
                        }
                    }
                    watch.Stop();
                    if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                        mLogger.Info($"{watch.ElapsedMilliseconds}");
                }
            }
            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetOrderComments(int docID)
        {
            DateTime startTime = DateTime.Now;
            DataSet dataSet = new DataSet();

            if (docID > 0 && GetUserIDFromSession() != 0)
            {
                var watch = Stopwatch.StartNew();
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;

                        sqlcmd.CommandText = $"SELECT CreateDateTime FROM Doc WHERE ID={docID}";
                        var firstLogwareInfoClientOrderNoteDate = new DateTime(2016, 12, 5, 14, 14, 40, 240);
                        if (Convert.ToDateTime(sqlcmd.ExecuteScalar()) >= firstLogwareInfoClientOrderNoteDate)
                        {
                            var resTable = new DataTable("Table");
                            resTable.Columns.Add(new DataColumn("ClientComment", typeof(string)));
                            resTable.Columns.Add(new DataColumn("DispatchWay", typeof(string)));
                            dataSet.Tables.Add(resTable);

                            var manager =
                                new ClientOrderDispatchWayHistoryManager(GetUserIDFromSession(), docID,
                                                        ConfigurationManager.AppSettings["MSQL"].ToString());
                            var dispatchWay = manager.GetDispatchWay();
                            var row = resTable.NewRow();
                            row["DispatchWay"] = string.IsNullOrEmpty(dispatchWay) ? "" : dispatchWay;
                            sqlcmd.CommandText =
                                 $"SELECT ISNULL(ClientComment, '') AS ClientComment FROM Doc WHERE ID={docID}";
                            var obj = sqlcmd.ExecuteScalar();
                            row["ClientComment"] = obj == null ? "" : obj.ToString();

                            resTable.Rows.Add(row);
                        }
                        else
                        {
                            var query =
                                "SELECT ISNULL(ClientComment, '') AS ClientComment,  " +
                                $"ISNULL((SELECT TOP 1 DispatchWay FROM dbo.Doc WHERE MasterID = {docID} " +
                                $"ORDER BY ID DESC),'') AS DispatchWay FROM dbo.Doc WHERE ID = {docID}";
                            try
                            {
                                SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                                dataAdapter.Fill(dataSet, "Table");
                            }
                            catch { }
                        }

                        if (IsAudit())
                        {
                            try
                            {
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, DocStrID, NewOutQty, GoodsRestID) VALUES (100, 'GetOrderComments', " +
                                    GetUserIDFromSession().ToString() + ", " + docID.ToString() + ", " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                sqlcmd.ExecuteNonQuery();

                            }
                            catch { }
                        }
                    }
                    connTSC.Close();
                    connTSC.Dispose();

                    watch.Stop();
                    if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                        mLogger.Info($"{watch.ElapsedMilliseconds}");
                }
            }

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public bool UpdateUserViewSettings(int showAllCrosses, int showAuxInfo)
        {
            DateTime startTime = DateTime.Now;
            bool result = false;
            int doc_id = 0, partner_id = GetUserIDFromSession();

            if (partner_id != 0)
            {
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    var watch = Stopwatch.StartNew();
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText = "SELECT ISNULL((SELECT TOP 1 ID FROM dbo.Doc WHERE Type = 60 AND PartnerID = " +
                            partner_id.ToString() + " ORDER BY ID DESC), 0)";
                        try
                        {
                            doc_id = (int)sqlcmd.ExecuteScalar();
                        }
                        catch
                        {
                            doc_id = 0;
                        }
                        if (doc_id > 0)
                        {
                            sqlcmd.CommandText = "UPDATE dbo.[Partner] SET ShowAllCrosses = " + (showAllCrosses == 0 ? "0" : "1") + ", ShowAuxInfo = " +
                                (showAuxInfo == 0 ? "0" : "1") + " WHERE ID = " + partner_id.ToString();
                            try
                            {
                                sqlcmd.ExecuteNonQuery();
                                if (Session["ShowCrossNumber"] == null)
                                {
                                    Session.Add("ShowCrossNumber", showAllCrosses == 0 ? false : true);
                                }
                                else
                                {
                                    bool original_show_cross_number = GetAccessAttribute("OriginalShowCrossNumber");
                                    if (original_show_cross_number)
                                    {
                                        Session["ShowCrossNumber"] = showAllCrosses == 0 ? false : true;
                                    }
                                }
                                if (Session["ShowAuxInfo"] == null)
                                {
                                    Session.Add("ShowAuxInfo", showAuxInfo == 0 ? false : true);
                                }
                                else
                                {
                                    Session["ShowAuxInfo"] = showAuxInfo == 0 ? false : true;
                                }
                                result = true;
                            }
                            catch
                            {
                                result = false;
                            }
                        }

                        if (IsAudit())
                        {
                            try
                            {
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, Comment) VALUES (100, 'UpdateUserViewSettings', " +
                                    GetUserIDFromSession().ToString() + ", " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    ", '" + result.ToString() + ";" + showAllCrosses.ToString() + ";" + showAuxInfo.ToString() + "')";
                                sqlcmd.ExecuteNonQuery();
                            }
                            catch { }
                        }
                    }
                    connTSC.Close();
                    connTSC.Dispose();

                    watch.Stop();
                    if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                        mLogger.Info($"{watch.ElapsedMilliseconds}");
                }
            }

            return result;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetForthcomingInfo(string number)
        {
            DateTime startTime = DateTime.Now;
            string currency_short_name, currency_rate, query;
            DataSet dataSet = new DataSet();
            int partner_id = GetUserIDFromSession();

            if (!String.IsNullOrEmpty(number) && partner_id != 0 && GetAccessAttribute("ShowForthcoming"))
            {
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    var watch = Stopwatch.StartNew();
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText = "SELECT ISNULL((SELECT CurrencyShortName FROM dbo.[Partner] WHERE ID = " + partner_id.ToString() + "), '')";
                        try
                        {
                            currency_short_name = (string)sqlcmd.ExecuteScalar();
                        }
                        catch
                        {
                            currency_short_name = "руб";
                        }
                    }
                    if (currency_short_name == "руб")
                    {
                        currency_rate = GetCurrencyRate().ToString().Replace(",", ".");
                    }
                    else
                    {
                        currency_rate = "1";
                    }
                    query = "SELECT t.ID, t.ArrivalDate1, t.ArrivalDate2, t.[Description], t.Number, " +
                        "CASE WHEN t.PartnerPrice > 0 THEN CONVERT(VARCHAR(40), ROUND(CONVERT(DECIMAL(30,2), t.PartnerPrice), 2)) ELSE 'уточняется' END AS Price, " +
                        //
                        "t.AvailableQty, t.WarehouseID, t.WarehouseName, " +
                        "t.WarehouseGroupID, t.WarehouseGroupName FROM (SELECT ID, ArrivalDate1, ArrivalDate2, " +
                        "ISNULL((SELECT TOP(1) [Description] FROM dbo.Goods WHERE Number = Forthcoming.Number ORDER BY ID DESC), '') AS [Description], " +
                        "Number, dbo.udfGetPartnerPrice(1, ISNULL((SELECT TOP 1 Name FROM dbo.[Partner] WHERE ID = " + partner_id.ToString() +
                        "), ''), Forthcoming.Number, " + currency_rate + ", 1) AS PartnerPrice, " +
                        "(Qty - ReservedQty) AS AvailableQty, Forthcoming.WarehouseID, (SELECT Name FROM dbo.Warehouse WHERE ID = Forthcoming.WarehouseID) AS WarehouseName, " +
                        "ISNULL((SELECT ID FROM dbo.WarehouseGroup WHERE ID = (SELECT TOP 1 WarehouseGroupID FROM dbo.Warehouse WHERE ID = Forthcoming.WarehouseID)),'') AS WarehouseGroupID, " +
                        "ISNULL((SELECT Name FROM dbo.WarehouseGroup WHERE ID = (SELECT TOP 1 WarehouseGroupID FROM dbo.Warehouse WHERE ID = Forthcoming.WarehouseID)),'') AS WarehouseGroupName " +
                        "FROM dbo.Forthcoming WHERE Number = '" + number + "' AND (Qty - ReservedQty) > 0  AND (IsVisible IS NOT NULL AND IsVisible = 1)) AS t " +  // <- "AND [Type] = 1020": 07.02.2014
                        "ORDER BY t.Number, t.ArrivalDate1, t.ArrivalDate2";
                    try
                    {
                        SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                        dataAdapter.Fill(dataSet, "Table");
                    }
                    catch { }

                    if (IsAudit())
                    {
                        try
                        {
                            using (SqlCommand sqlcmd2 = new SqlCommand())
                            {
                                sqlcmd2.Connection = connTSC;
                                sqlcmd2.CommandType = CommandType.Text;
                                sqlcmd2.CommandTimeout = 7200;
                                sqlcmd2.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, Number, NewOutQty, GoodsRestID) VALUES (100, 'GetForthcomingInfo', " +
                                    GetUserIDFromSession().ToString() + ", '" + (number.Length > 20 ? number.Substring(0, 20) : number) + "', " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                sqlcmd2.ExecuteNonQuery();
                            }
                        }
                        catch { }
                    }

                    connTSC.Close();
                    connTSC.Dispose();

                    watch.Stop();
                    if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                        mLogger.Info($"{watch.ElapsedMilliseconds}");
                }
            }

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public int ReserveForthcoming(int forthcomingID, string number, decimal price, string warehouseName, decimal qty, string comment)
        {
            DateTime startTime = DateTime.Now;
            int result = -1;
            string comment_to_write, currency_short_name;
            int partner_id = GetUserIDFromSession();
            decimal available_qty, reserve_sum;

            if (partner_id != 0 && GetAccessAttribute("ShowForthcoming") && forthcomingID > 0 &&
                !String.IsNullOrEmpty(number) && price >= 0 && qty > 0)
            {
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    var watch = Stopwatch.StartNew();
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText = "SELECT ISNULL((SELECT CurrencyShortName FROM dbo.[Partner] WHERE ID = " + partner_id.ToString() + "), '')";
                        try
                        {
                            currency_short_name = (string)sqlcmd.ExecuteScalar();
                        }
                        catch
                        {
                            currency_short_name = "руб";
                        }
                        comment_to_write = (String.IsNullOrEmpty(warehouseName) ? "" : warehouseName + ", ") +
                            (price > 0 ? price.ToString().Replace(",", ".") + " " + currency_short_name + ", " : "") +
                            (String.IsNullOrEmpty(comment) ? "" : comment);
                        if (comment_to_write.Length > 100)
                        {
                            comment_to_write = comment_to_write.Substring(0, 99);
                        }
                        //
                        sqlcmd.CommandText = "SELECT (ISNULL(Qty, 0) - ISNULL(ReservedQty, 0)) FROM dbo.Forthcoming WHERE ID = " + forthcomingID.ToString();
                        available_qty = Convert.ToDecimal((double)sqlcmd.ExecuteScalar());
                        if (qty > available_qty)
                        {
                            result = Convert.ToInt32(Decimal.Round(qty, 0) - Decimal.Round(available_qty, 0));
                        }
                        else
                        {
                            sqlcmd.CommandText = "INSERT INTO dbo.Reserve ([Login], ForthcomingID, PartnerID, Date, Number, " +
                                "Qty, Comment, CanUserDelete) VALUES ('" + (Session["userLogin"] ?? "") + "', " + forthcomingID.ToString() + ", " +
                                partner_id.ToString() + ", GETDATE(), '" + number + "', " + qty.ToString().Replace(",", ".") + ", '" +
                                comment_to_write + "', 1)";
                            try
                            {
                                sqlcmd.ExecuteNonQuery();
                                result = 0;
                            }
                            catch
                            {
                                result = -1;
                            }
                            if (result == 0)
                            {
                                try
                                {
                                    sqlcmd.CommandText = "SELECT ISNULL(SUM(Qty),0) FROM dbo.Reserve WHERE ForthcomingID = " + forthcomingID.ToString();
                                    reserve_sum = Convert.ToDecimal((double)sqlcmd.ExecuteScalar());
                                    //
                                    sqlcmd.CommandText = "UPDATE dbo.Forthcoming SET ReservedQty = " + Convert.ToString(reserve_sum).Replace(",", ".") +
                                        " WHERE ID = " + forthcomingID.ToString();
                                    sqlcmd.ExecuteNonQuery();
                                }
                                catch
                                {
                                    result = -1;
                                }
                            }
                        }

                        if (IsAudit())
                        {
                            try
                            {
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, GoodsRestID, Comment) VALUES (100, 'ReserveForthcoming', " +
                                    GetUserIDFromSession().ToString() + ", " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    ", " + result.ToString() + ", '" + forthcomingID.ToString() + ";" + number + ";" + price.ToString("F2").Replace(",", ".") +
                                    ";" + warehouseName + ";" + qty.ToString("F2").Replace(",", ".") + ";" + comment + "')";
                                sqlcmd.ExecuteNonQuery();
                            }
                            catch { }
                        }
                    }
                    watch.Stop();
                    if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                        mLogger.Info($"{watch.ElapsedMilliseconds}");
                }
            }

            return result;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetForthcomingReserveInfo()
        {
            DateTime startTime = DateTime.Now;
            string currency_short_name, currency_rate, query;
            DataSet dataSet = new DataSet();
            int partner_id = GetUserIDFromSession();

            if (partner_id != 0 && GetAccessAttribute("ShowForthcoming"))
            {
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    var watch = Stopwatch.StartNew();
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText = "SELECT ISNULL((SELECT CurrencyShortName FROM dbo.[Partner] WHERE ID = " + partner_id.ToString() + "), '')";
                        try
                        {
                            currency_short_name = (string)sqlcmd.ExecuteScalar();
                        }
                        catch
                        {
                            currency_short_name = "руб";
                        }
                    }
                    if (currency_short_name == "руб")
                    {
                        currency_rate = GetCurrencyRate().ToString().Replace(",", ".");
                    }
                    else
                    {
                        currency_rate = "1";
                    }
                    query = "SELECT ID, ForthcomingID, [Date] AS ReserveDate, " +
                        "(SELECT ArrivalDate1 FROM dbo.Forthcoming WHERE ID = Reserve.ForthcomingID) AS ArrivalDate1, " +
                        "(SELECT ArrivalDate2 FROM dbo.Forthcoming WHERE ID = Reserve.ForthcomingID) AS ArrivalDate2, " +
                        "ISNULL((SELECT TOP(1) [Description] FROM dbo.Goods WHERE Number = Reserve.Number ORDER BY ID DESC), '') AS [Description], " +
                        "Number, dbo.udfGetPartnerPrice(1, ISNULL((SELECT TOP 1 Name FROM dbo.[Partner] WHERE ID = " + partner_id.ToString() +
                        "), ''), Reserve.Number, " + currency_rate + ", 1) AS Price, Qty, " +
                        "ISNULL((SELECT Name FROM dbo.Warehouse WHERE ID = ISNULL((SELECT WarehouseID FROM dbo.Forthcoming WHERE ID = Reserve.ForthcomingID), 0)), '') AS WarehouseName, " +
                        "Comment, CanUserDelete FROM dbo.Reserve WHERE PartnerID = " + partner_id.ToString() + " ORDER BY [Date], ID";
                    try
                    {
                        SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                        dataAdapter.Fill(dataSet, "Table");
                    }
                    catch
                    {
                    }

                    if (IsAudit())
                    {
                        try
                        {
                            using (SqlCommand sqlcmd = new SqlCommand())
                            {
                                sqlcmd.Connection = connTSC;
                                sqlcmd.CommandType = CommandType.Text;
                                sqlcmd.CommandTimeout = 7200;
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, GoodsRestID) VALUES (100, 'GetForthcomingReserveInfo', " +
                                    GetUserIDFromSession().ToString() + ", " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                sqlcmd.ExecuteNonQuery();
                            }
                        }
                        catch { }
                    }

                    connTSC.Close();
                    connTSC.Dispose();

                    watch.Stop();
                    if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                        mLogger.Info($"{watch.ElapsedMilliseconds}");
                }
            }

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public bool DeleteForthcomingReserve(int reserveID)
        {
            DateTime startTime = DateTime.Now;
            bool result = false;
            int forthcomingID = 0, partner_id = GetUserIDFromSession();
            decimal reserve_sum;
            string supplier_name = "", str_shipment_id = "0", forthcoming_comment = "",
                str_arrival_date1 = "NULL", str_arrival_date2 = "NULL", str_reserve_date = "NULL", number = "",
                str_partner_id = "0", partner_name = "", str_reserved_qty = "0", reserved_comment = "";

            if (partner_id != 0 && GetAccessAttribute("ShowForthcoming"))
            {
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    var watch = Stopwatch.StartNew();
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandText = "SELECT ForthcomingID, PartnerID, " +
                            "ISNULL((SELECT Name FROM dbo.[Partner] WHERE ID = Reserve.PartnerID), ''), " +
                            "Date, Number, Qty, Comment FROM dbo.Reserve WHERE ID = " + reserveID.ToString();

                        try
                        {
                            using (SqlDataReader r = sqlcmd.ExecuteReader())
                            {
                                if (r.HasRows)
                                {
                                    r.Read();
                                    forthcomingID = r.IsDBNull(0) ? 0 : (int)r.GetSqlInt32(0);
                                    str_partner_id = r.IsDBNull(1) ? "0" : ((int)r.GetSqlInt32(1)).ToString();
                                    partner_name = r.IsDBNull(2) ? "" : (string)r.GetSqlString(2);
                                    str_reserve_date = r.IsDBNull(3) ? "NULL" : "'" + ((DateTime)((DateTime?)r.GetSqlDateTime(3))).ToString("MM.dd.yyyy HH:mm:ss") + "'";
                                    number = r.IsDBNull(4) ? "" : (string)r.GetSqlString(4);
                                    str_reserved_qty = r.IsDBNull(5) ? "0" : ((double)r.GetSqlDouble(5)).ToString().Replace(",", ".");
                                    reserved_comment = r.IsDBNull(6) ? "" : (string)r.GetSqlString(6);
                                }
                            }
                            result = true;
                        }
                        catch
                        {
                            result = false;
                        }
                        if (result)
                        {
                            sqlcmd.CommandText = "SELECT PartnerName, ShipmentID, Comment, ArrivalDate1, ArrivalDate2 " +
                                "FROM dbo.Forthcoming WHERE ID = " + forthcomingID.ToString();
                            using (SqlDataReader r = sqlcmd.ExecuteReader())
                            {
                                if (r.HasRows)
                                {
                                    r.Read();
                                    supplier_name = r.IsDBNull(0) ? "" : (string)r.GetSqlString(0);
                                    str_shipment_id = r.IsDBNull(1) ? "0" : ((int)r.GetSqlInt32(1)).ToString();
                                    forthcoming_comment = r.IsDBNull(2) ? "" : (string)r.GetSqlString(2);
                                    str_arrival_date1 = r.IsDBNull(3) ? "NULL" : "'" + ((DateTime)((DateTime?)r.GetSqlDateTime(3))).ToString("MM.dd.yyyy") + "'";
                                    str_arrival_date2 = r.IsDBNull(4) ? "NULL" : "'" + ((DateTime)((DateTime?)r.GetSqlDateTime(4))).ToString("MM.dd.yyyy") + "'";
                                }
                            }
                            sqlcmd.CommandText = "DELETE FROM dbo.Reserve WHERE ID = " + reserveID.ToString();
                            try
                            {
                                sqlcmd.ExecuteNonQuery();
                            }
                            catch
                            {
                                result = false;
                            }
                            if (result)
                            {
                                sqlcmd.CommandText = "INSERT INTO dbo.ReserveHistory ([Login], CreatorLogin, Type, " +
                                    "ForthcomingShipmentID, SupplierName, ForthcomingComment, ArrivalDate1, ArrivalDate2, " +
                                    "ReserveDate, Number, PartnerID, PartnerName, PrevForthcomingComment, PrevQty, PrevComment, " +
                                    "NewQty, NewComment, DocStrID) VALUES ('" + (Session["userLogin"] ?? "") + "', '" +
                                    (Session["userLogin"] ?? "") + "', 30, " + str_shipment_id + ", '" + supplier_name +
                                    "', '', " + str_arrival_date1 + ", " + str_arrival_date2 + ", " + str_reserve_date + ", '" +
                                    number + "', " + str_partner_id + ", '" + partner_name + "', '" + forthcoming_comment + "', " +
                                    str_reserved_qty + ", '" + reserved_comment + "', 0, '', 0)";
                                try
                                {
                                    sqlcmd.ExecuteNonQuery();
                                }
                                catch
                                { }
                                try
                                {
                                    sqlcmd.CommandText = "SELECT ISNULL(SUM(Qty),0) FROM dbo.Reserve WHERE ForthcomingID = " + forthcomingID.ToString();
                                    reserve_sum = Convert.ToDecimal((double)sqlcmd.ExecuteScalar());
                                    //
                                    sqlcmd.CommandText = "UPDATE dbo.Forthcoming SET ReservedQty = " + Convert.ToString(reserve_sum).Replace(",", ".") +
                                        " WHERE ID = " + forthcomingID.ToString();
                                    sqlcmd.ExecuteNonQuery();
                                }
                                catch
                                {
                                    result = false;
                                }

                                if (IsAudit())
                                {
                                    try
                                    {
                                        sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, DocStrID, NewOutQty, Comment) VALUES (100, 'DeleteForthcomingReserve', " +
                                            GetUserIDFromSession().ToString() + ", " + reserveID.ToString() + ", " +
                                            Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                            ", '" + result.ToString() + "')";
                                        sqlcmd.ExecuteNonQuery();
                                    }
                                    catch { }
                                }
                            }
                        }
                        watch.Stop();
                        if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                            mLogger.Info($"{watch.ElapsedMilliseconds}");
                    }
                }
            }

            return result;
        }

        [WebMethod(EnableSession = true)]
        public int SetForOrder(int[] docStrIds, int isForOrder)
        {
            DateTime startTime = DateTime.Now;
            int result = 0, partner_id = GetUserIDFromSession();
            var isForOrderVal = isForOrder == 0 ? 0 : 1;

            if (partner_id != 0 && docStrIds.Length > 0)
            {
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    var watch = Stopwatch.StartNew();
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        var builder = new StringBuilder();
                        foreach (var id in docStrIds)
                            builder.Append($"{id},");
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandText =
                            $"UPDATE DocStr SET IsForOrder = {isForOrderVal} WHERE " +
                            $"ID IN ({builder.ToString().TrimEnd(',')})";

                        //sqlcmd.CommandType = CommandType.StoredProcedure;
                        //sqlcmd.CommandTimeout = 7200;
                        //sqlcmd.CommandText = "spSetForOrder";
                        //sqlcmd.Parameters.Clear();
                        ////
                        //SqlParameter prm_id = new SqlParameter();
                        //prm_id.Direction = ParameterDirection.Input;
                        //prm_id.ParameterName = "@doc_str_id";
                        //prm_id.SqlDbType = SqlDbType.Int;
                        //prm_id.Value = id;
                        //sqlcmd.Parameters.Add(prm_id);
                        ////
                        //SqlParameter prm_is_for_order = new SqlParameter();
                        //prm_is_for_order.Direction = ParameterDirection.Input;
                        //prm_is_for_order.ParameterName = "@is_for_order";
                        //prm_is_for_order.SqlDbType = SqlDbType.Int;
                        //prm_is_for_order.Value = isForOrder;
                        //sqlcmd.Parameters.Add(prm_is_for_order);
                        ////
                        //SqlParameter prm_result = new SqlParameter();
                        //prm_result.Direction = ParameterDirection.Output;
                        //prm_result.ParameterName = "@result";
                        //prm_result.SqlDbType = SqlDbType.Int;
                        //sqlcmd.Parameters.Add(prm_result);
                        try
                        {
                            result = sqlcmd.ExecuteNonQuery();
                            //result = (int)prm_result.Value;
                        }
                        catch
                        {
                            result = 0;
                        }

                        if (IsAudit())
                        {
                            try
                            {
                                sqlcmd.CommandType = CommandType.Text;
                                sqlcmd.Parameters.Clear();
                                sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, GoodsRestID, Comment) VALUES (100, 'SetForOrder', " +
                                    GetUserIDFromSession().ToString() + ", " +
                                    Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                    ", " + docStrIds.ToString() + ", '" + isForOrder.ToString() + "')";
                                sqlcmd.ExecuteNonQuery();
                            }
                            catch { }
                        }
                    }

                    watch.Stop();
                    if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                        mLogger.Info($"{watch.ElapsedMilliseconds}");
                }
            }
            return result;
        }

        [WebMethod(EnableSession = true)]
        //public DataSet GetPartnerActionGoods(int partnerId)
        public DataSet GetPartnerActionGoods()
        {
            DateTime startTime = DateTime.Now;

            var partnerId = GetUserIDFromSession();
            DataSet dataSet = new DataSet();

            using (var conn = GetDBConnection())
            {
                if (partnerId == 0 || conn == null || conn.State != ConnectionState.Open)
                    return new DataSet();

                var watch = Stopwatch.StartNew();
                var partnerName = "";

                using (var command = conn.CreateCommand())
                {
                    command.CommandText =
                        $"SELECT ISNULL(Name, '') FROM dbo.[Partner] WHERE ID = {partnerId}";
                    var obj = command.ExecuteScalar();
                    partnerName = obj == null ? "" : obj.ToString();
                }
                if (string.IsNullOrEmpty(partnerName))
                    return new DataSet();

                var query = "SELECT GoodsRest.Number, [Cross].Manufacturer, GoodsRest.[Description], " +
                    $"dbo.udfGetWebPartnerRestPrice(1, 1, 1, '{partnerName}', GoodsRest.ID, ISNULL(dbo.udfReturnCurrencyRate(GETDATE()), 0)) AS Price " +
                    "FROM GoodsRest LEFT OUTER JOIN [Cross] ON GoodsRest.Number = [Cross].Number " +
                    "WHERE (GoodsRest.Rest > 0) AND (GoodsRest.Number IN (" +
                    $"SELECT Number FROM SpecialPrice WHERE(PartnerID = {partnerId}))) AND " +
                    "(GoodsRest.WarehouseID IN (SELECT WarehouseID  FROM PartnerClosedWarehouses " +
                    $"WHERE(PartnerID = {partnerId}) AND(AccessRight = 1)))";

                try
                {
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(query, conn);
                    dataAdapter.Fill(dataSet, "Table");
                }
                catch { }
                watch.Stop();
                if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                    mLogger.Info($"{watch.ElapsedMilliseconds}");
            }

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetActionData()
        {
            DateTime startTime = DateTime.Now;
            string query, currency_short_name = "", partner_name = "";
            DataSet dataSet = new DataSet();

            int partner_id = GetUserIDFromSession();
            if (partner_id != 0)
            {
                SqlConnection connTSC = GetDBConnection();
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    var watch = Stopwatch.StartNew();
                    using (SqlCommand sqlcmd = new SqlCommand())
                    {
                        sqlcmd.Connection = connTSC;
                        sqlcmd.CommandTimeout = 7200;
                        sqlcmd.CommandType = CommandType.Text;
                        sqlcmd.CommandText = "SELECT ISNULL(Name, ''), ISNULL(CurrencyShortName, '') FROM dbo.[Partner] WHERE ID = " + partner_id.ToString();
                        using (SqlDataReader r = sqlcmd.ExecuteReader())
                        {
                            if (r.HasRows)
                            {
                                r.Read();
                                partner_name = r.IsDBNull(0) ? "" : (string)r.GetSqlString(0);
                                currency_short_name = r.IsDBNull(1) ? "EUR" : (string)r.GetSqlString(1);
                            }
                        }
                    }

                    if (!string.IsNullOrEmpty(partner_name) && !string.IsNullOrEmpty(currency_short_name))
                    {
                        query = "SELECT t3.Manufacturer, t3.Number, t3.[Description], t3.Price, t3.Amount, " +
                            "t3.WarehouseID, t3.WarehouseName, t3.AccessRight, t3.OnStore, " +
                            "dbo.udfGetAggregatedID(t3.Number, t3.Price, t3.Price, t3.WarehouseID) AS ViewID, t3.CreateDateTime FROM (SELECT " +
                            "ISNULL((SELECT TOP 1 Manufacturer FROM dbo.[Cross] WHERE Number = t2.Number ORDER BY ID DESC), '') AS Manufacturer, " +
                            "t2.CreateDateTime, t2.Number, " +
                            "ISNULL((SELECT TOP(1) [Description] FROM dbo.Goods WHERE Number = t2.Number ORDER BY ID DESC), '') AS [Description], " +
                            "t2.Price, t2.WarehouseID, ISNULL((SELECT Name FROM dbo.Warehouse WHERE ID = t2.WarehouseID), '' ) AS WarehouseName, " +
                            "SUM(t2.Rest) AS Amount, " +
                            "ISNULL((SELECT AccessRight FROM dbo.PartnerClosedWarehouses WHERE WarehouseID = t2.WarehouseID AND PartnerID = " +
                             partner_id.ToString() + "), 3) AS  AccessRight, " +
                            "1 AS OnStore, ISNULL((SELECT SortID FROM dbo.Warehouse WHERE ID = t2.WarehouseID), 0) AS WarehouseSortID FROM (" +
                            "SELECT t.CreateDateTime, t.Number, ";

                        //--
                        //CASE @currency_short_name
                        //    WHEN 'EUR' THEN dbo.udfGetWebPartnerRestPrice(1, 1, 1, @partner_name, t.GoodsRestID, 1)
                        //    ELSE dbo.udfGetWebPartnerRestPrice(1, 1, 1,@partner_name, t.GoodsRestID, ISNULL(dbo.udfReturnCurrencyRate(GETDATE()), 0))
                        //END AS Price,
                        if (currency_short_name == "EUR")
                        {
                            query += "dbo.udfGetWebPartnerRestPrice(1, 1, 1, '" + partner_name + "', t.GoodsRestID, 1) AS Price,";
                        }
                        else
                        {
                            query += "dbo.udfGetWebPartnerRestPrice(1, 1, 1, '" + partner_name +
                                "', t.GoodsRestID, ISNULL(dbo.udfReturnCurrencyRate(GETDATE()), 0)) AS Price,";
                        }

                        query += " t.WarehouseID, t.Rest FROM (SELECT ActionPrices.CreateDateTime, " +
                            "ActionPrices.Number, GoodsRest.ID AS GoodsRestID, GoodsRest.WarehouseID, GoodsRest.Rest, " +
                            "ISNULL((SELECT TOP 1 ShowInClientPrice FROM dbo.Goods WHERE Number = ActionPrices.Number ORDER BY ID DESC), 1) AS ShowInClientPrice " +
                            "FROM dbo.ActionPrices LEFT OUTER JOIN dbo.GoodsRest ON GoodsRest.Number = ActionPrices.Number " +
                            "WHERE GoodsRest.Rest > 0 AND GoodsRest.[Sign] = 'N' AND GoodsRest.Number NOT LIKE 'RTS%' " +
                            //-- проверка есть или нет для этого клиента товары по акциям. Для этого используется связка Клиент-Manager-Акционные товары в базе
                            "AND ActionPrices.Manager = ISNULL((SELECT Manager FROM dbo.[Partner] WHERE ID = " + partner_id.ToString() + "), '') " +
                            //-- 
                            ") AS t WHERE t.ShowInClientPrice = 1 ) AS t2 GROUP BY t2.CreateDateTime, t2.Number, t2.Price, t2.WarehouseID) AS t3 " +
                            "WHERE t3.AccessRight IN (1,2) " +
                            "ORDER BY t3.AccessRight, t3.WarehouseSortID, t3.WarehouseID, t3.Price, t3.Manufacturer, t3.Number";
                        try
                        {
                            SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                            dataAdapter.Fill(dataSet, "Table");
                        }
                        catch { }

                        if (IsAudit())
                        {
                            try
                            {
                                using (SqlCommand sqlcmd = new SqlCommand())
                                {
                                    sqlcmd.Connection = connTSC;
                                    sqlcmd.CommandType = CommandType.Text;
                                    sqlcmd.CommandTimeout = 7200;
                                    sqlcmd.CommandText = "INSERT INTO dbo.[Log] ([Type], Stage, DocID, NewOutQty, GoodsRestID) VALUES (100, 'GetActionData', " +
                                        GetUserIDFromSession().ToString() + ", " +
                                        Decimal.Round((decimal)((TimeSpan)(DateTime.Now - startTime)).TotalSeconds, 3).ToString().Replace(",", ".") +
                                        ", " + (dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0).ToString() + ")";
                                    sqlcmd.ExecuteNonQuery();
                                }
                            }
                            catch { }
                        }
                    }
                    connTSC.Close();
                    connTSC.Dispose();

                    watch.Stop();
                    if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                        mLogger.Info($"{watch.ElapsedMilliseconds}");
                }
            }
            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetPartnerName()
        {
            var res = new DataSet();

            int partnerId = GetUserIDFromSession();
            if (partnerId == 0)
                return res;

            var watch = Stopwatch.StartNew();
            using (var connTSC = GetDBConnection())
            {
                if (connTSC != null && connTSC.State == ConnectionState.Open)
                {
                    try
                    {
                        var query = $"SELECT ISNULL(Name,''),ISNULL(City,'') FROM Partner WHERE ID={partnerId}";
                        var adapter = new SqlDataAdapter(query, connTSC);
                        adapter.Fill(res, "PartnerName");
                    }
                    catch { }
                }
            }

            watch.Stop();
            if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                mLogger.Info($"{watch.ElapsedMilliseconds}");
            return res;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetPartnerInfo()
        {
            var res = new DataSet();

            int partnerId = GetUserIDFromSession();
            if (partnerId == 0)
                return res;

            using (var connTSC = GetDBConnection())
            {
                if (connTSC == null || connTSC.State != ConnectionState.Open)
                    return res;

                var watch = Stopwatch.StartNew();
                var query = "";

                try
                {
                    query = "SELECT ISNULL(FullName,'') AS FullName, ISNULL(LegalAddress,'') AS LegalAddress " +
                        "FROM dbo.PartnerAgent " +
                         $"WHERE ID={partnerId}";
                    var adapter = new SqlDataAdapter(query, connTSC);
                    adapter.Fill(res, "Agents");
                }
                catch { }

                try
                {
                    query = $"SELECT ISNULL(FullName,'') AS FullName FROM dbo.PartnerReceiver WHERE " +
                        $"PartnerID={partnerId} ";
                    var adapter = new SqlDataAdapter(query, connTSC);
                    adapter.Fill(res, "Recievers");
                }
                catch { }
                watch.Stop();
                if (Convert.ToBoolean(ConfigurationManager.AppSettings["MisureMethodDuration"]))
                    mLogger.Info($"{watch.ElapsedMilliseconds}");
            }

            return res;
        }

        #endregion Public Methods

        #region External Methods

        [WebMethod(EnableSession = true)]
        private bool CheckExternalUser(string externalLogin, string externalPassword)
        {
            bool result = false;
            string grantedExternalLogin, grantedExternalPassword;

            try
            {
                grantedExternalLogin = ConfigurationManager.AppSettings["GrantedExternalLogin"].ToString();
                grantedExternalPassword = ConfigurationManager.AppSettings["GrantedExternalPassword"].ToString();
            }
            catch
            {
                grantedExternalLogin = "";
                grantedExternalPassword = "";
            }
            if (!string.IsNullOrEmpty(externalLogin) && !string.IsNullOrEmpty(externalPassword) &&
                !string.IsNullOrEmpty(grantedExternalLogin) && !string.IsNullOrEmpty(grantedExternalPassword) &&
                externalLogin == grantedExternalLogin && externalPassword == grantedExternalPassword)
            {
                result = true;
            }

            return result;
        }

        [WebMethod(EnableSession = true)]
        public DataSet ApplicationComponentForExternalCall(string number, string externalLogin, string externalPassword)
        {
            string query;
            DataSet dataSet = new DataSet();

            SqlConnection connTSC = GetDBConnection();
            if (connTSC != null && connTSC.State == ConnectionState.Open && CheckExternalUser(externalLogin, externalPassword))
            {
                query = "SELECT DISTINCT ISNULL(GoodsNumber,'') AS GoodsNumber FROM dbo.GoodsComponents WHERE ComponentNumber = '" +
                    number + "' ORDER BY GoodsNumber";
                try
                {
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                    dataAdapter.Fill(dataSet, "Table");
                }
                catch { }
                connTSC.Close();
                connTSC.Dispose();
            }

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet ApplicationComponentByCrossGroupIDForExternalCall(int crossGroupID, string externalLogin, string externalPassword)
        {
            string query;
            DataSet dataSet = new DataSet();

            SqlConnection connTSC = GetDBConnection();
            if (connTSC != null && connTSC.State == ConnectionState.Open && CheckExternalUser(externalLogin, externalPassword))
            {
                query = "SELECT DISTINCT ISNULL(GoodsNumber, '') AS PriorNumber FROM dbo.GoodsComponents WHERE ComponentNumber IN " +
                    "(SELECT Number FROM dbo.[Cross] WHERE CrossGroupID = " + crossGroupID.ToString() + ") ORDER BY PriorNumber";
                try
                {
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                    dataAdapter.Fill(dataSet, "Table");
                }
                catch { }
                connTSC.Close();
                connTSC.Dispose();
            }

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetMakerListForExternalCall(string externalLogin, string externalPassword)
        {
            string query;
            DataSet dataSet = new DataSet();

            SqlConnection connTSC = GetDBConnection();
            if (connTSC != null && connTSC.State == ConnectionState.Open && CheckExternalUser(externalLogin, externalPassword))
            {
                query = "SELECT DISTINCT Maker FROM dbo.Car WHERE Maker IS NOT NULL AND LEN(Maker) > 0 ORDER BY Maker";
                try
                {
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                    dataAdapter.Fill(dataSet, "Table");
                }
                catch { }
                connTSC.Close();
                connTSC.Dispose();
            }

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetModelListForExternalCall(string maker, string externalLogin, string externalPassword)
        {
            string query;
            DataSet dataSet = new DataSet();

            SqlConnection connTSC = GetDBConnection();
            if (connTSC != null && connTSC.State == ConnectionState.Open && CheckExternalUser(externalLogin, externalPassword))
            {
                query = "SELECT DISTINCT Model FROM dbo.Car WHERE (Model IS NOT NULL AND LEN(Model) > 0) AND Maker = '" +
                    maker + "' ORDER BY Model";
                try
                {
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                    dataAdapter.Fill(dataSet, "Table");
                }
                catch { }
                connTSC.Close();
                connTSC.Dispose();
            }

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetEngineListForExternalCall(string maker, string model, string externalLogin, string externalPassword)
        {
            string query;
            DataSet dataSet = new DataSet();

            SqlConnection connTSC = GetDBConnection();
            if (connTSC != null && connTSC.State == ConnectionState.Open && CheckExternalUser(externalLogin, externalPassword))
            {
                query = "SELECT DISTINCT Engine FROM dbo.Car WHERE (Engine IS NOT NULL AND LEN(Engine) > 0) " +
                    (maker.Length > 0 ? "AND Maker = '" + maker + "'" : "") + (model.Length > 0 ? "AND Model = '" + model + "'" : "") +
                    "ORDER BY Engine";
                try
                {
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                    dataAdapter.Fill(dataSet, "Table");
                }
                catch { }
                connTSC.Close();
                connTSC.Dispose();
            }

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet FindApplicationForExternalCall(int crossGroupID, string externalLogin, string externalPassword)
        {
            string query;
            DataSet dataSet = new DataSet();

            SqlConnection connTSC = GetDBConnection();
            if (connTSC != null && connTSC.State == ConnectionState.Open && CheckExternalUser(externalLogin, externalPassword))
            {
                query = "SELECT Maker, Model, Engine, Comment, " +
                    "CONVERT(VARCHAR(4), YEAR(YearFrom)) + '.' + REPLICATE('0', 2 - LEN(CONVERT(VARCHAR(2), MONTH(YearFrom)))) + CONVERT(VARCHAR(2), MONTH(YearFrom)) + '-' + " +
                    "ISNULL(CONVERT(VARCHAR(4), YEAR(YearTo)) + '.' + REPLICATE('0', 2 - LEN(CONVERT(VARCHAR(2), MONTH(YearTo)))) +CONVERT(VARCHAR(2), MONTH(YearTo)),'') AS Years " +
                    "FROM dbo.Car WHERE ID IN (SELECT CarID FROM dbo.GoodsApplication WHERE Number IN " +
                    "(SELECT DISTINCT Number FROM dbo.[Cross] WHERE CrossGroupID = " + crossGroupID.ToString() + "))" +
                    "ORDER BY Maker, Model, Engine, YearFrom, YearTo, Comment";
                try
                {
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                    dataAdapter.Fill(dataSet, "Table");
                }
                catch { }
                connTSC.Close();
                connTSC.Dispose();
            }
            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet FindComponentsForExternalCall(string number, string externalLogin, string externalPassword)
        {
            string query;
            DataSet dataSet = new DataSet();

            SqlConnection connTSC = GetDBConnection();
            if (connTSC != null && connTSC.State == ConnectionState.Open && CheckExternalUser(externalLogin, externalPassword))
            {
                query = "SELECT [Type], ComponentNumber, ISNULL((SELECT TOP 1 Number FROM dbo.[Cross] WHERE CrossGroupID = " +
                    "(SELECT TOP 1 CrossGroupID FROM dbo.[Cross] WHERE Number = GoodsComponents.ComponentNumber) " +
                    "AND LEN(IsPrior) > 0 ORDER BY ID),'') AS PriorNumber,Comment " +
                    "FROM dbo.GoodsComponents WHERE GoodsNumber = '" + number + "' ORDER BY Type, ComponentNumber";
                try
                {
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                    dataAdapter.Fill(dataSet, "Table");
                }
                catch { }
                connTSC.Close();
                connTSC.Dispose();
            }
            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetApplicationForExternalCall(string maker, string model, string engine, string externalLogin, string externalPassword)
        {
            string query, s;
            DataSet dataSet = new DataSet();

            SqlConnection connTSC = GetDBConnection();
            if (connTSC != null && connTSC.State == ConnectionState.Open && CheckExternalUser(externalLogin, externalPassword))
            {
                query = "SELECT cars.Maker, cars.Model, cars.Engine, " +
                    //with nulls
                    "st.GoodsApplicationComment AS StarterComment, st.Number AS StarterNumber, " +
                    "alt.GoodsApplicationComment AS AlternatorComment, alt.Number AS AlternatorNumber, " +
                    //with spaces instead of nulls
                    //"ISNULL(st.GoodsApplicationComment, '') AS StarterComment, ISNULL(st.Number, '') AS StarterNumber, " +
                    //"ISNULL(alt.GoodsApplicationComment, '') AS AlternatorComment, ISNULL(alt.Number, '') AS AlternatorNumber, " +
                    //
                    "heaters.HeaterComment, heaters.HeaterNumber, " +
                    //
                    "cars.Years FROM (SELECT Car.ID, Car.Maker, Car.Model, Car.Engine, Car.YearFrom, Car.YearTo, " +
                    "CONVERT(VARCHAR(4), YEAR(YearFrom)) + '.' + REPLICATE('0', 2 - LEN(CONVERT(VARCHAR(2), MONTH(YearFrom)))) + CONVERT(VARCHAR(2), MONTH(YearFrom)) + '-' + " +
                    "ISNULL(CONVERT(VARCHAR(4), YEAR(YearTo)) + '.' + REPLICATE('0', 2 - LEN(CONVERT(VARCHAR(2), MONTH(YearTo)))) + CONVERT(VARCHAR(2), MONTH(YearTo)), '') AS Years " +
                    "FROM dbo.Car WHERE  Car.ID > 0" +
                    //prms:
                    (maker.Length > 0 ? " AND Car.Maker = '" + maker + "'" : "") + (model.Length > 0 ? " AND Car.Model = '" + model + "'" : "") +
                    (engine.Length > 0 ? " AND Car.Engine = '" + engine + "'" : "") +
                    //
                    ") AS cars LEFT OUTER JOIN " +
                    "(SELECT Car.ID,ISNULL((SELECT TOP 1 Number FROM dbo.[Cross] WHERE CrossGroupID = " +
                    "(SELECT TOP 1 CrossGroupID FROM dbo.[Cross] WHERE Number = GoodsApplication.Number) AND LEN(IsPrior) > 0 ORDER BY ID),'') AS Number, " +
                    "ISNULL(GoodsApplication.Comment, '') AS GoodsApplicationComment " +
                    "FROM dbo.Car LEFT OUTER JOIN  dbo.GoodsApplication ON GoodsApplication.CarID = Car.ID " +
                    "WHERE Car.ID > 0 " +
                    //prms:
                    (maker.Length > 0 ? "AND Car.Maker = '" + maker + "'" : "") + (model.Length > 0 ? "AND Car.Model = '" + model + "'" : "") +
                    (engine.Length > 0 ? "AND Car.Engine = '" + engine + "'" : "") +
                    //old code:
                    //"AND (SELECT TOP 1 GoodsGroupID FROM dbo.Goods WHERE Number = GoodsApplication.Number ORDER BY ID DESC) = 16 " +
                    //new code:
                    " AND 16 IN (SELECT ISNULL((SELECT DISTINCT TOP 1 GoodsGroupID FROM dbo.Goods " +
                    "WHERE Number IN (SELECT Number FROM dbo.[Cross] WHERE CrossGroupID IN (" +
                    "SELECT CrossGroupID FROM dbo.[Cross] WHERE Number = GoodsApplication.Number)) AND GoodsGroupID = 16), 0))" +
                    //
                    ") AS st ON st.ID = cars.ID LEFT OUTER JOIN (SELECT Car.ID, ISNULL((SELECT TOP 1 Number FROM dbo.[Cross] WHERE CrossGroupID = " +
                    "(SELECT TOP 1 CrossGroupID FROM dbo.[Cross] WHERE Number = GoodsApplication.Number) AND LEN(IsPrior) > 0 ORDER BY ID),'') AS Number, " +
                    "ISNULL(GoodsApplication.Comment, '') AS GoodsApplicationComment " +
                    "FROM dbo.Car LEFT OUTER JOIN  dbo.GoodsApplication ON GoodsApplication.CarID = Car.ID " +
                    "WHERE Car.ID > 0 " +
                    //prms:
                    (maker.Length > 0 ? "AND Car.Maker = '" + maker + "'" : "") + (model.Length > 0 ? "AND Car.Model = '" + model + "'" : "") +
                    (engine.Length > 0 ? "AND Car.Engine = '" + engine + "'" : "") +
                    //old code:
                    //"AND (SELECT TOP 1 GoodsGroupID FROM dbo.Goods WHERE Number = GoodsApplication.Number ORDER BY ID DESC) = 5 " +
                    //new code:
                    " AND 5 IN (SELECT ISNULL((SELECT DISTINCT TOP 1 GoodsGroupID FROM dbo.Goods " +
                    "WHERE Number IN (SELECT Number FROM dbo.[Cross] WHERE CrossGroupID IN (" +
                    "SELECT CrossGroupID FROM dbo.[Cross] WHERE Number = GoodsApplication.Number)) AND GoodsGroupID = 5), 0))" +
                    //new code (20.05.2015):
                    ") AS alt ON alt.ID = cars.ID " +
                    "LEFT OUTER JOIN (" +
                    "SELECT CarID, ISNULL(GoodsApplication.Comment, '') AS HeaterComment, ISNULL(GoodsApplication.Number, '') AS HeaterNumber " +
                    "FROM dbo.GoodsApplication WHERE [Description] LIKE '%отопит%') AS heaters " +
                    "ON cars.ID = heaters.CarID " +
                    //
                    "ORDER BY cars.Maker, cars.Model, cars.YearFrom, cars.YearTo";
                try
                {
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                    dataAdapter.Fill(dataSet, "Table");
                }
                catch { }
                connTSC.Close();
                connTSC.Dispose();
            }
            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public int GetImagesCountForExternalCall(int crossGroupID, string externalLogin, string externalPassword)
        {
            int result = 0;
            int partner_id = GetUserIDFromSession();

            SqlConnection connTSC = GetDBConnection();
            if (connTSC != null && connTSC.State == ConnectionState.Open && CheckExternalUser(externalLogin, externalPassword))
            {
                using (SqlCommand sqlcmd = new SqlCommand())
                {
                    sqlcmd.Connection = connTSC;
                    sqlcmd.CommandType = CommandType.Text;
                    sqlcmd.CommandTimeout = 7200;
                    sqlcmd.CommandText = "SELECT COUNT(*) FROM dbo.GoodsImages WHERE Number IN (SELECT DISTINCT SearchNumber FROM dbo.[Cross] " +
                        "WHERE CrossGroupID = " + crossGroupID.ToString() + ") OR Number IN (SELECT DISTINCT Number FROM " +
                        "dbo.[Cross] WHERE CrossGroupID = " + crossGroupID.ToString() + ")";
                    try
                    {
                        result = (int)sqlcmd.ExecuteScalar();
                    }
                    catch
                    {
                        result = 0;
                    }
                }
                connTSC.Close();
                connTSC.Dispose();
            }

            return result;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetImagesForExternalCall(int crossGroupID, string externalLogin, string externalPassword)
        {
            string query;
            DataSet dataSet = new DataSet();

            SqlConnection connTSC = GetDBConnection();
            if (connTSC != null && connTSC.State == ConnectionState.Open && CheckExternalUser(externalLogin, externalPassword))
            {
                query = "SELECT ID, Name, [Image], " +
                    "(SELECT TOP 1 IsPrior FROM dbo.[Cross] WHERE dbo.[Cross].Number = " +
                    "dbo.GoodsImages.Number OR dbo.[Cross].SearchNumber = dbo.GoodsImages.Number AND " +
                    "CrossGroupID = " + crossGroupID.ToString() + ") AS IsPrior FROM dbo.GoodsImages WHERE Number IN " +
                    "(SELECT DISTINCT SearchNumber FROM dbo.[Cross] WHERE CrossGroupID = " + crossGroupID.ToString() +
                    ") OR Number IN (SELECT DISTINCT Number FROM dbo.[Cross] WHERE CrossGroupID = " + crossGroupID.ToString() + ") " +
                    "ORDER BY IsPrior DESC, Number, Name, ID";
                try
                {
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                    dataAdapter.Fill(dataSet, "Table");
                }
                catch { }
                connTSC.Close();
                connTSC.Dispose();
            }

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetImageByNumberForExternalCall(string number, string externalLogin, string externalPassword)
        {
            string query;
            DataSet dataSet = new DataSet();

            SqlConnection connTSC = GetDBConnection();
            if (connTSC != null && connTSC.State == ConnectionState.Open && CheckExternalUser(externalLogin, externalPassword))
            {
                query = "SELECT [Image] FROM dbo.GoodsImages WHERE Number = '" + number + "'";
                try
                {
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                    dataAdapter.Fill(dataSet, "Table");
                }
                catch { }
                connTSC.Close();
                connTSC.Dispose();
            }

            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet FindNumberForExternalCall(string number, bool only, string externalLogin, string externalPassword)
        {
            string query;
            DataSet dataSet = new DataSet();

            SqlConnection connTSC = GetDBConnection();
            if (connTSC != null && connTSC.State == ConnectionState.Open && CheckExternalUser(externalLogin, externalPassword))
            {
                number = number.Replace(" ", "").Replace(".", "").Replace("-", "").Replace(",", "").Replace(@"\", "").Replace("/", "");

                query = "SELECT t.Number, t.Manufacturer, t.CrossGroupID, " +
                    "CASE WHEN (t.[Description] IS NULL) OR (LEN(t.[Description]) = 0) THEN " +
                    "ISNULL((SELECT TOP (1) [Description] FROM dbo.CrossComment WHERE CrossGroupID = t.CrossGroupID ORDER BY ID DESC), '') " +
                    "ELSE t.[Description] END AS [Description] FROM (" +
                    "SELECT TOP(200) Number, Manufacturer, CrossGroupID, " +
                    "(SELECT TOP(1) [Description] FROM dbo.Goods WHERE Number = [Cross].Number ORDER BY ID DESC) AS [Description] " +
                    "FROM dbo.[Cross] WHERE SearchNumber " + (only ? "= '" + number + "'" : "LIKE '%" + number + "%'") +
                    " AND Number NOT LIKE 'RTS%') AS t ORDER BY t.Number, t.Manufacturer";
                try
                {
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                    dataAdapter.Fill(dataSet, "Table");
                    //
                    WriteWebRequest(number, dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0);
                }
                catch { }
                connTSC.Close();
                connTSC.Dispose();
            }
            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public DataSet FindNumberInCrossForExternalCall(string number, string externalLogin, string externalPassword)
        {
            string query;
            DataSet dataSet = new DataSet();

            SqlConnection connTSC = GetDBConnection();
            if (connTSC != null && connTSC.State == ConnectionState.Open && CheckExternalUser(externalLogin, externalPassword))
            {
                number = number.Replace("'", "");
                number = number.Replace(" ", "").Replace(".", "").Replace("-", "").Replace(",", "").Replace(@"\", "").Replace("/", "");

                query = "SELECT t.Number, t.Manufacturer, t.CrossGroupID, " +
                    "CASE WHEN (t.[Description] IS NULL) OR (LEN(t.[Description]) = 0) THEN " +
                    "ISNULL((SELECT TOP (1) [Description] FROM dbo.CrossComment WHERE CrossGroupID = t.CrossGroupID ORDER BY ID DESC), '') " +
                    "ELSE t.[Description] END AS [Description] FROM (" +
                    "SELECT TOP(200) Number, Manufacturer, CrossGroupID, " +
                    "(SELECT TOP(1) [Description] FROM dbo.Goods WHERE Number = [Cross].Number ORDER BY ID DESC) AS [Description] " +
                    "FROM dbo.[Cross] WHERE SearchNumber LIKE '%" + number + "%' AND Number NOT LIKE 'RTS%') " +
                    " AS t ORDER BY t.Number, t.Manufacturer";
                try
                {
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                    dataAdapter.Fill(dataSet, "Table");
                    //
                    WriteWebRequest(number, dataSet.Tables.Count > 0 ? dataSet.Tables[0].Rows.Count : 0);
                }
                catch { }
                connTSC.Close();
                connTSC.Dispose();
            }
            return dataSet;
        }

        [WebMethod(EnableSession = true)]
        public string GetCrossCommentForExternalCall(int CrossGroupID, string externalLogin, string externalPassword)
        {
            string result = "";
            int partner_id = GetUserIDFromSession();

            SqlConnection connTSC = GetDBConnection();
            if (connTSC != null && connTSC.State == ConnectionState.Open && CheckExternalUser(externalLogin, externalPassword))
            {
                using (SqlCommand sqlcmd = new SqlCommand())
                {
                    sqlcmd.Connection = connTSC;
                    sqlcmd.CommandType = CommandType.Text;
                    sqlcmd.CommandTimeout = 7200;
                    sqlcmd.CommandText = "SELECT ISNULL((SELECT [Value] FROM dbo.CrossComment WHERE CrossGroupID = " + CrossGroupID.ToString() + "), '')";
                    try
                    {
                        result = (string)sqlcmd.ExecuteScalar();
                    }
                    catch
                    {
                        result = "";
                    }
                }
                connTSC.Close();
                connTSC.Dispose();
            }

            return result;
        }

        [WebMethod(EnableSession = true)]
        public DataSet GetExternalMNDList(int crossGroupID, string externalLogin, string externalPassword)
        {
            string query;
            DataSet dataSet = new DataSet();

            SqlConnection connTSC = GetDBConnection();
            if (connTSC != null && connTSC.State == ConnectionState.Open && CheckExternalUser(externalLogin, externalPassword))
            {
                query = "SELECT t.Manufacturer, t.[Number], CASE WHEN LEN(Description1) > 0 THEN Description1 " +
                    "ELSE Description2 END AS [Description], " +
                    "(SELECT COUNT(*) FROM GoodsComponents WHERE GoodsNumber = t.Number) AS AmountComponents " +
                    "FROM (" +
                    "SELECT DISTINCT Manufacturer, Number, " +
                    "ISNULL((SELECT TOP 1 [Description] FROM dbo.Goods WHERE Number  = [Cross].Number),'') AS Description1, " +
                    "ISNULL((SELECT TOP 1 [Description] FROM dbo.Goods WHERE Number IN " +
                    "(SELECT Number FROM dbo.[Cross] WHERE CrossGroupID = " + crossGroupID.ToString() + ") AND LEN([Description]) > 0 " +
                    "ORDER BY LEN([Description])), '') AS Description2, " +
                    "ISNULL((SELECT TOP 1 ShowInClientPrice FROM dbo.Goods WHERE Number = [Cross].Number ORDER BY ID DESC), 1) AS ShowInClientPrice " +
                    "FROM dbo.[Cross] WHERE CrossGroupID = " + crossGroupID.ToString() +
                    ") AS t  WHERE t.ShowInClientPrice = 1 AND LEN(t.Manufacturer) > 0 AND LEN(t.Number ) > 0 AND t.Number NOT LIKE 'RTS%' " +
                    "ORDER BY t.Manufacturer, t.[Number]";
                try
                {
                    SqlDataAdapter dataAdapter = new SqlDataAdapter(query, connTSC);
                    dataAdapter.Fill(dataSet, "Table");
                }
                catch { }
                connTSC.Close();
                connTSC.Dispose();
            }
            return dataSet;
        }

        #endregion External Methods
    }
}