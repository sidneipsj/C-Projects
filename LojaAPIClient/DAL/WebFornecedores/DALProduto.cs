using System;
using System.Web.UI.Adapters;
using System.Web.UI.HtmlControls;
using System.Collections.Generic;
using System.Configuration;
using System.Text;
using SQLHelperv2;
using Negocio;
using System.Data.Common;
using System.Data;
using Misc;

namespace DAL
{
   public class DALProduto        
   {       
      // Retorna uma lista de produtos da autorização
      public static List<Produtos> GetProdAutor(string pAutorizacaoId)
      {
         string sql = " Select PRECO_UNI, PRECO_TAB, QTDE, DESCRICAO, MOV_PROD2.PROD_ID, CODINBS, MOV_ID, COMREC" +
                        " FROM MOV_PROD2 JOIN PRODUTOS ON PRODUTOS.PROD_ID = MOV_PROD2.PROD_ID " +
                        " WHERE MOV_PROD2.AUTORIZACAO_ID = @autorizacaoId AND MOV_PROD2.CANCELADO <> 'S'";
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("autorizacaoId", pAutorizacaoId));
         BD BD = new BD();
         SafeDataReader dr = BD.GetDataReader(sql, ps);
         List<Produtos> prodList = new List<Produtos>();
         try
         {
             while (dr.Read())
             {
                 Produtos prod = new Produtos();
                 prod.Descricao = dr.GetString("DESCRICAO");
                 prod.PrecoTabela = dr.GetDouble("PRECO_TAB");
                 prod.PrecoVenda = dr.GetDouble("PRECO_UNI");
                 prod.Qtde = dr.GetInt32("QTDE");
                 prod.Produto_id = dr.GetInt32("PROD_ID");
                 prod.ComRec = dr.GetString("COMREC");

                 prodList.Add(prod);
             }

         }
         finally
         {
             dr.Close();
         }
         return prodList;
      }

      // Busca no banco, produtos pela descricao. Se a Descrição tiver menos ou 3 caracteres, procura por produtos que comecem com a descricao, 
      // caso contrario, que contenham a descrição.
      public static DataTable ListaProd(string pDescricaoProd)
      {
         string sql = " SELECT prod.prod_id, precos.preco_final, precos.preco_sem_desconto, precos.data, UPPER(prod.descricao) AS descricao  " +
                       " FROM Produtos prod JOIN Precos ON precos.prod_id = prod.prod_id " +
                       " WHERE prod.apagado <> 'S' AND COALESCE(prod.ENVIADO_FARMACIA,'N') <> 'S' AND COALESCE(prod.CODINBS,'') <> '' ";
         
         if (pDescricaoProd.Length <= 3)
            sql += " AND prod.descricao STARTING WITH @descr";
         else
            sql += " AND prod.descricao CONTAINING @descr";
         
         sql += " ORDER BY prod.descricao, precos.data ";
         
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("descr", pDescricaoProd));

         BD BD = new BD();
         return BD.GetDataTable(sql, ps);
      }

      public static string[] GetDescricaoProduto(string codbarras)
      {
         string sql = " SELECT FIRST 1 COALESCE(prod.descricao,'') AS descricao, COALESCE(prod.preco_vnd,prod.preco_final,0) AS preco";
         sql += " FROM Produtos prod";
         sql += " JOIN Barras bar ON bar.prod_id = prod.prod_id";
         sql += " WHERE prod.apagado <> 'S'";
         sql += " AND bar.barras = @codbarras";

         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("codbarras", codbarras));

         BD BD = new BD();
         DataRow row = BD.GetOneRow(sql, ps);

         string[] retorno = new string[2];

         retorno[0] = string.Empty;
         retorno[1] = "0,00";

         if (row != null)
         {
            retorno[0] = row["descricao"].ToString();
            retorno[1] = Convert.ToDouble(row["preco"]).ToString("N2");
         }

         return retorno;
      }

      // Insere produto na autorização
      public static void InsertMov(string pAutorizacaoId, Produtos pProduto)
      {
         MontadorSql mont = new MontadorSql("mov_prod2", MontadorType.Insert);
         mont.AddField("AUTORIZACAO_ID", pAutorizacaoId);
         mont.AddField("PROD_ID", pProduto.Produto_id);
         mont.AddField("QTDE", pProduto.Qtde);
         mont.AddField("PRECO_UNI", pProduto.PrecoVenda.ToString().Replace(",", "."));
         mont.AddField("PRECO_TAB", pProduto.PrecoTabela.ToString().Replace(",", "."));
         mont.AddField("COMREC", pProduto.ComRec);
         mont.AddField("CRM", pProduto.Crm);

         BD BD = new BD();
         BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());
      }

      // Insere um novo Produto na tabela "produtos" e em seguida o preço do mesmo na tabela "precos"
      public static int InsertProd(string Descricao, double PrecoFinal, double PrecoTabela)
      {
         string sql = "SELECT MAX(prod_id) AS prod_id FROM Produtos";
         BD BD = new BD();
         int prod_id = BD.ExecuteScalar<int>(0, sql, null) + 1;
         try
         {
            MontadorSql mont = new MontadorSql("produtos", MontadorType.Insert);
            mont.AddField("prod_id", (prod_id));
            mont.AddField("codinbs", "9991000009999");
            mont.AddField("descricao", Descricao);
            mont.AddField("sn", "");
            mont.AddField("flagnome", "");
            mont.AddField("mt", "M");
            mont.AddField("apagado", "N");
            mont.AddField("data", DateTime.Now.ToString("dd/MM/yyyy"));
            
            BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());

         }
         catch (Exception ex)
         {
            throw new Exception("Erro ao inserir produto: " + ex.Message);
         }

         try
         {
            MontadorSql mont = new MontadorSql("precos", MontadorType.Insert);
            mont.AddField("prod_id", prod_id);
            mont.AddField("data", DateTime.Now.ToString("dd/MM/yyyy"));
            mont.AddField("preco_final", PrecoFinal);
            mont.AddField("preco_sem_desconto", PrecoTabela);
            BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());
         }
         catch (Exception ex)
         {
            throw new Exception("Erro ao inserir preços: " + ex.Message);
         }
         return prod_id;
      }

   }
}
