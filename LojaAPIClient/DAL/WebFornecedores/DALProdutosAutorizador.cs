using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using SQLHelperv2;

namespace DAL
{
    public class DALProdutosAutorizador
    {
        //Alterado para SqlServer
        public static DataSet ProdutoCodigo(string prodCodigo)
        {
            string sql = "SELECT DISTINCT PROD_CODIGO AS PROD_COD, PROD_DESCR, PRE_VALOR AS PROD_VALOR, LABORATORIO, ALT_LIBERADA, DEPTO_CODIGO FROM PRODUTOS_AUTORIZADOR"
                                 + " WHERE PROD_CODIGO LIKE '%" + prodCodigo + "%' AND APAGADO = 'N' ORDER BY PROD_DESCR";
            BD BD = new BD();
            return BD.GetDataSet(sql, null);
        }

        //Alterado para SqlServer
        public static DataSet ProdutoDescricao(string prodDescricao)
        {
            string sql = "SELECT DISTINCT PROD_CODIGO AS PROD_COD, PROD_DESCR, PRE_VALOR AS PROD_VALOR, LABORATORIO, ALT_LIBERADA, DEPTO_CODIGO FROM PRODUTOS_AUTORIZADOR"
                                 + " WHERE PROD_DESCR LIKE '%" + prodDescricao + "%' AND APAGADO = 'N' ORDER BY PROD_DESCR";
            BD BD = new BD();
            return BD.GetDataSet(sql, null);
        }

        public static double GetVerificaDesconto(string empresId, string codDeptp)
        {
            string sql = "SELECT PORC_DESC FROM DEPTO_DESCONTOS WHERE EMPRES_ID = " + empresId + " AND DEPTO_CODIGO = " + codDeptp;

            try
            {
                BD BD = new BD();
                return Convert.ToDouble(BD.GetOneRow(sql, null).ItemArray[0]);
            }
            catch // o sum é null
            {
                return 0;
            }
        }

    }
}
