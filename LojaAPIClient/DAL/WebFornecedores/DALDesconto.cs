using System;
using System.Data;
using System.Configuration;
using System.Web;
using System.Web.Security;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Web.UI.WebControls.WebParts;
using System.Web.UI.HtmlControls;
using SQLHelperv2;

namespace DAL
{
   public class DALDesconto
   {
      //Pega o desconto do Credenciado (Farmacia) - Carrega um novo DataTable para preencher o subReport
      public DataTable aRecebDesconto(int cred_id, double valor_pgto)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("cred_id", cred_id));
         ps.Add(new Fields("valor", valor_pgto));
         BD BD = new BD();
         DataTable tabDes = BD.GetDataTable("SELECT 0 AS pagamento_cred_id, id, descricao, valor, (CASE WHEN fixa = 'S' THEN 'Sim' ELSE 'Não' END) AS fixa FROM Get_descontoscred(@cred_id,@valor)", ps);
         return tabDes;
      }

      // Pega o desconto do Credenciado (Farmacia) dentro dos pagamentos indicados - Carrega um novo DataTable para preencher o subReport
      public DataTable RecebidoDesconto(string nPagamento, int cred_id)
      {
         string   sql = " SELECT des.pagamento_cred_id, des.historico AS descricao, des.valor";
                  sql += " FROM Pagamento_cred_desc des";
                  sql += " JOIN Pagamento_cred pag ON des.pagamento_cred_id = pag.pagamento_cred_id";
                  sql += " WHERE des.pagamento_cred_id IN(" + nPagamento + ")";                  
                  sql += " UNION ALL";                  
                  sql += " SELECT pag.pagamento_cred_id, 'REPASSE DE VENDAS' AS descricao, pag.repasse ";
                  sql += " FROM Pagamento_cred pag ";
                  sql += " WHERE pag.pagamento_cred_id IN(" + nPagamento + ")";
                  sql += " AND pag.repasse > 0.00";

                  BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }
   }
}
