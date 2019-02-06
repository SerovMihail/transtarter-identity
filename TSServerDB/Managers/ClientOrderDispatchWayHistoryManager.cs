using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Web;

namespace TSServerDB.Managers
{
    /// <summary>
    /// Реализует функционал для получения истории способа доставки заказа
    /// </summary>
    internal class ClientOrderDispatchWayHistoryManager
    {
        /// <summary>
        /// Идентификатор клиента
        /// </summary>
        private int mClientId;
        /// <summary>
        /// Идентификатор заказа(документа)
        /// </summary>
        private int mDocId;
        /// <summary>
        /// Строка подключения к БД
        /// </summary>
        private string mConnectionString;

        /// <summary>
        /// Создаёт новый экземпляр класса <c>ClientOrderDispatchWayHistoryManager</c>
        /// </summary>
        /// <param name="clientId">Идентификатор клиента</param>
        /// <param name="docId">Идентификатор заказа(документа)</param>
        /// <param name="connectionString">Строка подключения к БД</param>
        public ClientOrderDispatchWayHistoryManager(int clientId, int docId, string connectionString)
        {
            mClientId = clientId;
            mDocId = docId;
            mConnectionString = connectionString;
        }

        /// <summary>
        /// Получить способ доставки заказа клиенту
        /// </summary>
        /// <returns>Способ доставки или пустая строка, если не удалось найти необходимые данные</returns>
        public string GetDispatchWay()
        {
            //var docStage = "";
            //var neededStages = new List<string>
            //        {
            //            "в пункте выдачи",
            //            "отправлен клиенту",
            //            "получен клиентом"
            //        };

            using (var conn = GetNewConnection())
            {
                using (var cmd = conn.CreateCommand())
                {
                    //cmd.CommandText = $"SELECT TOP 1 ISNULL(Stage,'') FROM Doc WHERE ID={mDocId}";
                    //docStage = cmd.ExecuteScalar().ToString();
                    //if (!neededStages.Contains(docStage))
                    //    return "";

                    cmd.CommandText = $"SELECT TOP 1 * FROM ClientOrderNote WHERE DocId={mDocId} " +
                        "AND NoteType=1 ORDER BY Id DESC";

                    using (var reader = cmd.ExecuteReader())
                    {
                        if (!reader.Read())
                            return "";

                        if (reader["PickupTime"] != DBNull.Value)
                            return GetByPickupTime(Convert.ToDateTime(reader["PickupTime"]));

                        if (reader["IssuePointId"] != DBNull.Value &&
                            Convert.ToInt32(reader["IssuePointId"]) > 0)
                            return GetByIssuePoint(Convert.ToInt32(reader["IssuePointId"]));

                        if (reader["AddressId"] != DBNull.Value && Convert.ToInt32(reader["AddressId"]) > 0)
                            return GetByAddress(Convert.ToInt32(reader["AddressId"]));

                        if (reader["TransportCompanyId"] != DBNull.Value &&
                            Convert.ToInt32(reader["TransportCompanyId"]) > 0)
                            return GetByTransportCompany(Convert.ToInt32(reader["TransportCompanyId"]));
                    }
                }
            }

            return "";
        }

        /// <summary>
        /// Получить способ доставки по адресу доставки
        /// </summary>
        /// <param name="addressId">Идентификатор адреса доставки</param>
        /// <returns>Способ получения по адресу или пустая строка</returns>
        private string GetByAddress(int addressId)
        {
            var fullAddress = GetFullAddress(addressId);
            return string.IsNullOrEmpty(fullAddress)
                 ? ""
                 : $"Доставка по адресу: {fullAddress}";
        }

        /// <summary>
        /// Получить способ доставки транспортной компанией
        /// </summary>
        /// <param name="partnerDeliveryParameterId">Идентификатор записи параметров доставки клиенту</param>
        /// <returns>Способ оставки транспортной компанией или пустая строка</returns>
        private string GetByTransportCompany(int partnerDeliveryParameterId)
        {
            using (var conn = GetNewConnection())
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = "SELECT ISNULL(Name,'') FROM Partner WHERE ID IN ( " +
                        "SELECT TransportCompanyID FROM PartnerDeliveryParameters " +
                        $"WHERE ID={partnerDeliveryParameterId})";
                    var obj = cmd.ExecuteScalar();
                    var tcName = obj == null ? "" : obj.ToString();
                    cmd.CommandText = "SELECT ISNULL(InvoiceNumber,'') FROM ClientOrderNote WHERE NoteType=2 AND " +
                        $"InvoiceNumber IS NOT NULL AND InvoiceNumber<>'' AND DocId={mDocId}";
                    obj = cmd.ExecuteScalar();
                    var invoiceNumber = obj == null ? "" : obj.ToString();
                    var res = "";
                    if (!string.IsNullOrEmpty(tcName))
                        res = $"доставка транспортной компанией '{tcName}' ";
                    if (!string.IsNullOrEmpty(invoiceNumber))
                        res += $", № транспортной накладной - {invoiceNumber}.";

                    return !string.IsNullOrEmpty(res)
                        ? char.ToUpper(res[0]) + res.Substring(1).Trim()
                        : "";
                }
            }
        }

        /// <summary>
        /// Получить способ доставки самовывозом
        /// </summary>
        /// <param name="pickupTime">Дата самомывоза</param>
        /// <returns>Способ доставки самовывозом</returns>
        private string GetByPickupTime(DateTime pickupTime)
        {
            return $"Самовывоз {pickupTime.ToString("dd.MM.yyyy HH:mm")}";
        }

        /// <summary>
        /// Получить способ доставки в пункт выдачи
        /// </summary>
        /// <param name="partnerDeliveryParameterId">Идентификатор записи параметров доставки клиенту</param>
        /// <returns>Способ доставки в пункт выдачи или пустая строка</returns>
        private string GetByIssuePoint(int partnerDeliveryParameterId)
        {
            using (var conn = GetNewConnection())
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText =
                        $"SELECT ISNULL(IssuePointId,-1) FROM PartnerDeliveryParameters WHERE ID={partnerDeliveryParameterId}";
                    var obj = cmd.ExecuteScalar();
                    var addressId = obj == null ? -1 : Convert.ToInt32(obj);
                    if (addressId < 1)
                        return "";

                    var fullAddress = GetFullAddress(addressId);
                    return string.IsNullOrEmpty(fullAddress)
                        ? ""
                        : $"Доставка в пункт выдачи по адресу: {fullAddress}";
                }
            }
        }

        /// <summary>
        /// Получить полный адрес
        /// </summary>
        /// <param name="addressId">Идентификатор</param>
        /// <returns>Полный адрес или пустая строка, если не удалось найти адрес в БД, добавлен хак, берется другое поле для адреса</returns>
        private string GetFullAddress(int addressId)
        {
            using (var conn = GetNewConnection())
            {
                using (var cmd = conn.CreateCommand())
                {
                    cmd.CommandText = $"SELECT ISNULL(FullAddress,'')  FROM Addresses WHERE ID=(SELECT AddressID FROM PartnerDeliveryParameters WHERE ID = {addressId} )";
                    var obj = cmd.ExecuteScalar();
                    return obj == null
                        ? ""
                        : obj.ToString();
                }
            }
        }

        /// <summary>
        /// Создание нового открытого соединения с БД
        /// </summary>
        /// <returns>Созданно подключение к БД</returns>
        private SqlConnection GetNewConnection()
        {
            var conn = new SqlConnection(mConnectionString);
            conn.Open();
            return conn;
        }
    }
}