using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using SQLHelperv2;
using Negocio;

namespace DAL
{
    public class DALCredComanda
    {
        public static int GeraComID(int cred_id)
        {
            string sql = "SELECT MAX(COM_ID) AS COM_ID FROM CRED_COMANDA WHERE CRED_ID = " + cred_id;
            BD BD = new BD();
            DataTable tabela = BD.GetDataTable(sql, null);
            if (tabela.Rows[0][0].ToString() == "")
            {
                return 1;
            }
            else
            {
                return Convert.ToInt32(tabela.Rows[0][0].ToString()) + 1;
            }
        }

        public static void InserirComandas(int cred_id, string data, string hora, int comanda, int com_id, int item, int qtde, decimal valor)
        {
            try
            {
                BD BD = new BD();

                MontadorSql mont = new MontadorSql("cred_comanda", MontadorType.Insert);
                mont.AddField("cred_id", cred_id);
                mont.AddField("data", data);
                mont.AddField("hora", hora);
                mont.AddField("comanda", comanda);
                mont.AddField("com_id", com_id);
                mont.AddField("com_item", item);
                mont.AddField("qtde", qtde);
                mont.AddField("valor", valor);
                BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());
            }
            catch (Exception ex)
            {
                new Exception("Erro ao inserir comanda: " + ex.Message);
            }
        }

        public static bool UpdateQtde(int cred_id, int comanda, int com_id, int qtde)
        {
            BD BD = new BD();
            if (BD.ExecuteNoQuery("UPDATE CRED_COMANDA SET QTDE = " + qtde + " WHERE CRED_ID = " + cred_id + " AND COMANDA = " + comanda + " AND COM_ID = " + com_id, null) == 1)
                return true;
            return false;
        }

        public static bool UpdateValor(int cred_id, int comanda, int com_id, decimal valor)
        {
            BD BD = new BD();
            if (BD.ExecuteNoQuery("UPDATE CRED_COMANDA SET VALOR = " + valor + " WHERE CRED_ID = " + cred_id + " AND COMANDA = " + comanda + " AND COM_ID = " + com_id, null) == 1)
                return true;
            return false;
        }

        public static void DelComanda(int cred_id, int comanda, int com_id)
        {
            BD BD = new BD();
            BD.ExecuteNoQuery("DELETE FROM CRED_COMANDA WHERE CRED_ID = " + cred_id + " AND COMANDA = " + comanda + " AND COM_ID = " + com_id, null);
        }

        public static DataTable SelecionarComanda(int cred_id, int comanda, int com_id)
        {
            string sql = "SELECT * FROM CRED_COMANDA WHERE CRED_ID = " + cred_id + " AND COMANDA = " + comanda + " AND COM_ID = " + com_id + " ORDER BY COM_ITEM";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static int ContItem(int cred_id, int comanda, int com_id)
        {
            string sql = "SELECT MAX(COM_ITEM) AS COM_ITEM FROM CRED_COMANDA WHERE CRED_ID = " + cred_id + " AND COMANDA = " + comanda + " AND COM_ID = " + com_id; 
            BD BD = new BD();
            DataTable tabela = BD.GetDataTable(sql, null);
            if (tabela.Rows[0][0].ToString() == "")
            {
                return 1;
            }
            else
            {
                return Convert.ToInt32(tabela.Rows[0][0].ToString()) + 1;
            }
        }

        public static int UltimoID(int cred_id, int comanda)
        {
            string sql = "SELECT MAX(COM_ID) AS COM_ID, MAX(DATA) AS DATA FROM CRED_COMANDA WHERE CRED_ID = " + cred_id + " AND COMANDA = " + comanda;
            BD BD = new BD();
            DataTable tabela = BD.GetDataTable(sql, null);
            if (tabela.Rows[0][0].ToString() == "")
            {
                return 0;
            }
            else
                return Convert.ToInt32(tabela.Rows[0][0].ToString());
            //}
            //else
            //    return 0;
        }


        public static void DelItem(int cred_id, int comanda, int com_id, int item)
        {
            BD BD = new BD();
            BD.ExecuteNoQuery("DELETE FROM CRED_COMANDA WHERE CRED_ID = " + cred_id + " AND COMANDA = " + comanda + " AND COM_ID = " + com_id + " AND COM_ITEM = " + item, null);
        }

        public static DataTable RelatorioAnalitico(Extratos pExtrato, int pCredenciadoId)
        {
            string sql = "SELECT DATA, SUM(VALOR * QTDE) AS VALOR FROM CRED_COMANDA WHERE CRED_ID = " + pCredenciadoId + " AND DATA BETWEEN @datai AND @dataf GROUP BY DATA ORDER BY DATA";

            SqlParamsList ps = new SqlParamsList();
            ps.Add(new Fields("@credId", pCredenciadoId));
            ps.Add(new Fields("@datai", pExtrato.DataIni.ToString("dd/MM/yyyy")));
            ps.Add(new Fields("@dataf", pExtrato.DataFim.ToString("dd/MM/yyyy")));

            BD BD = new BD();
            return BD.GetDataTable(sql, ps);
        }


        public static DataTable RelatorioSintetico(Extratos pExtrato, int pCredenciadoId)
        {
            string sql = "SELECT DATA, HORA, COMANDA, COM_ID, QTDE, VALOR FROM CRED_COMANDA WHERE CRED_ID = " + pCredenciadoId + " AND DATA BETWEEN @datai AND @dataf "
            + "ORDER BY DATA, HORA, COMANDA, COM_ID";

            SqlParamsList ps = new SqlParamsList();
            ps.Add(new Fields("@credId", pCredenciadoId));
            ps.Add(new Fields("@datai", pExtrato.DataIni.ToString("dd/MM/yyyy")));
            ps.Add(new Fields("@dataf", pExtrato.DataFim.ToString("dd/MM/yyyy")));

            BD BD = new BD();
            return BD.GetDataTable(sql, ps);
        }
    }
}
