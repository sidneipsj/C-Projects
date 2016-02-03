using System;
using System.Collections.Generic;
using System.Text;
using SQLHelperv2;
using System.Data;

namespace DAL
{
    public class DALRecargaOperadoras
    {
        //Todos os Metodos alterado para SqlServer
        public static DataTable CarregarDDD(string operadora)
        {
            string sql = "SELECT DISTINCT DDD FROM RECARGA_OPERADORAS WHERE DESCRICAO = '" + operadora + "' ORDER BY DDD";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }


        public static DataTable CarregarValores(string nomeOp, int ddd)
        {
            string sql = "SELECT DISTINCT VALOR FROM RECARGA_OPERADORAS WHERE DESCRICAO = '" + nomeOp
                + "' AND DDD = " + ddd + " ORDER BY VALOR";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static void InserirOperadoras(int verTabela, int codOperadora, string nomeOp, int ddd, double valor)
        {
            try
            {
                BD BD = new BD();

                MontadorSql mont = new MontadorSql("recarga_operadoras", MontadorType.Insert);
                mont.AddField("versao_tabela", verTabela);
                mont.AddField("cod_operadora", codOperadora);
                mont.AddField("descricao", nomeOp);
                mont.AddField("ddd", ddd);
                mont.AddField("valor", valor);
                BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());
            }
            catch (Exception ex)
            {
                new Exception("Erro ao inserir operadoras: " + ex.Message);
            }
        }

        public static void DelOperadoras()
        {
            BD BD = new BD();
            BD.ExecuteNoQuery("DELETE FROM RECARGA_OPERADORAS", null);
        }

        public static DataTable DadosOperadora(string nomeOp)
        {
            string sql = "SELECT DISTINCT VERSAO_TABELA, COD_OPERADORA FROM RECARGA_OPERADORAS WHERE DESCRICAO = '" + nomeOp + "'";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable Operadoras()
        {
            string sql = "SELECT DISTINCT DESCRICAO FROM RECARGA_OPERADORAS ORDER BY DESCRICAO";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }
    }
}
