using System;
using System.Collections.Generic;
using System.Text;
using SQLHelperv2;

namespace DAL
{
    public class DALRecargaConfirmadas
    {
        //Todos os Metodos Alterado para SqlServer
        public static void InserirPreAuto(string data, decimal valor, long nsuTan, string dtAuto, string cartao)
        {
            try
            {
                BD BD = new BD();

                MontadorSql mont = new MontadorSql("recarga_confirmadas", MontadorType.Insert);
                mont.AddField("data_auto", data);
                mont.AddField("valor", valor);
                mont.AddField("recarga_realizada", 'N');
                mont.AddField("nsu_tan", nsuTan);
                mont.AddField("trans_confirmada", 'S');
                mont.AddField("data_trans", dtAuto);
                mont.AddField("cartao", cartao);
                BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());
            }
            catch (Exception ex)
            {
                throw new Exception("Erro ao inserir pre-autorizacao confirmada: " + ex.Message);
            }

        }

        public static bool UpTransacao(string data, long nsuTan)
        {
            BD BD = new BD();
            if (BD.ExecuteNoQuery("UPDATE RECARGA_CONFIRMADAS SET TRANS_CONFIRMADA = 'N' WHERE DATA_AUTO = '" + data + "' AND NSU_TAN = " + nsuTan, null) == 1)
                return true;
            return false;
        }

        public static bool UpRConfirmadas(string data, string nsuUnilojas, string nsuOperadora, string operadora, int ddd, int numero, long nsuTan)
        {
            try
            {
                BD BD = new BD();
                if (BD.ExecuteNoQuery("UPDATE RECARGA_CONFIRMADAS SET NSU_UNILOJAS = '" + nsuUnilojas + "', NSU_OPERADORA = '" + nsuOperadora + "'"
                    + ", OPERADORA = '" + operadora + "', DDD = " + ddd + ", NUMERO = " + numero
                    + ", RECARGA_REALIZADA = 'S' WHERE DATA_AUTO = '" + data + "' AND NSU_TAN = " + nsuTan, null) == 1)
                    return true;
                return false;
            }
            catch (Exception ex)
            {
                throw new Exception("Erro ao inserir recarga confirmada: " + ex.Message);
            }
        }

        public static void DelConfirmadas()
        {
            BD BD = new BD();
            BD.ExecuteNoQuery("DELETE FROM RECARGA_CONFIRMADAS", null);
        }

        public static void PosRecargaConf(string data, string nsu_unilojas, string nsu_operadora, string operadora, int ddd, int numero, decimal valor, int cred_id)
        {
            try
            {
                BD BD = new BD();

                MontadorSql mont = new MontadorSql("recarga_confirmadas", MontadorType.Insert);
                mont.AddField("data_auto", data);
                mont.AddField("nsu_unilojas", nsu_unilojas);
                mont.AddField("nsu_operadora", nsu_operadora);
                mont.AddField("operadora", operadora);
                mont.AddField("ddd", ddd);
                mont.AddField("numero", numero);
                mont.AddField("valor", valor);
                mont.AddField("recarga_realizada", "S");
                mont.AddField("tipo_venda", "AVISTA");
                mont.AddField("cred_id", cred_id);
                BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());
            }
            catch (Exception ex)
            {
                throw new Exception("Erro ao inserir recarga confirmada: " + ex.Message);
            }

        }

        public static void PosAutoConf(string data, string nsu_tan, string cartao, int cred_id, decimal valor, DateTime dataVenda)
        {
            try
            {
                BD BD = new BD();

                MontadorSql mont = new MontadorSql("recarga_confirmadas", MontadorType.Insert);
                mont.AddField("data_auto", data);
                mont.AddField("nsu_tan", nsu_tan);
                mont.AddField("cartao", cartao);
                mont.AddField("cred_id", cred_id);
                mont.AddField("tipo_venda", "CONVENIO");
                mont.AddField("trans_confirmada", "S");
                mont.AddField("recarga_realizada", "N");
                mont.AddField("valor", valor);
                mont.AddField("data_trans", dataVenda);
                BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());
            }
            catch (Exception ex)
            {
                throw new Exception("Erro ao inserir recarga confirmada: " + ex.Message);
            }

        }

        public static bool RecargaConfirmada(long nsu)
        {
            BD BD = new BD();
            bool retorno = false;
            SafeDataReader dr = BD.GetDataReader("SELECT RECARGA_REALIZADA FROM RECARGA_CONFIRMADAS WHERE NSU_TAN = " + nsu, null);

            try
            {
                if (dr.Read())
                {
                    retorno = dr.GetString(0) == "S";
                }
            }
            finally
            {
                dr.Close();
            }

            return retorno;
        }

        public static bool UpRCancelada(long nsuTan)
        {
            try
            {
                BD BD = new BD();
                if (BD.ExecuteNoQuery("UPDATE RECARGA_CONFIRMADAS SET TRANS_CONFIRMADA = 'C' WHERE NSU_TAN = " + nsuTan, null) == 1)
                    return true;
                return false;
            }
            catch (Exception ex)
            {
                throw new Exception("Erro ao inserir recarga confirmada: " + ex.Message);
            }
        }
    }
}
