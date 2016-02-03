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
   public class DALTitular
   {
      // Pega os titulares contidos nas faturas indicadas Credenciado (Farmacia) 
      // Carrega um novo DataTable para preencher os dados do Titulares no Relatorio</returns>
      public DataTable aRecebTitular(string faturasId, int cred_id)
      {
         string sqlTitu = " SELECT cc.fatura_id, cv.chapa, cv.titular, cc.data, cc.autorizacao_id, " +
                          " cc.digito,cc.debito-cc.credito as valor, cv.empres_id, pagamento_cred_id " +
                          " FROM Contacorrente cc" +
                          " JOIN Conveniados cv on cv.conv_id = cc.conv_id" +
                          " WHERE cred_id = " + cred_id + " AND cc.fatura_id IN (" + faturasId + ")" +
                          " AND cc.pagamento_cred_id = 0 " +
                          " ORDER BY cc.data, cc.autorizacao_id";

         BD BD = new BD();
         return BD.GetDataTable(sqlTitu, null);
      }

      // Pega os titulares contidos nas faturas indecadas Credenciado (Farmacia) 
      // dentro dos pagamentos e faturas indicados
      // Carrega um novo DataTable para preencher os dados do Titulares no Relatorio</returns>
      public DataTable RecebidoTitular(string nPagamento, int cred_id)
      {
         string   sql  = " SELECT conv.empres_id, conv.titular, conv.chapa, cc.pagamento_cred_id,";
                  sql += " CASE WHEN pagpor.paga_cred_por_id = 2 THEN 0 ELSE cc.fatura_id END AS fatura_id,";
                  sql += " cc.autorizacao_id, cc.digito, (cc.debito-cc.credito) AS valor, cc.data";         
                  sql += " FROM Contacorrente cc";
                  sql += " JOIN Pagamento_cred pag ON pag.pagamento_cred_id = cc.pagamento_cred_id";
                  sql += " JOIN Paga_cred_por pagpor ON pagpor.paga_cred_por_id = pag.paga_cred_por_id";
                  sql += " LEFT JOIN Conveniados conv ON conv.conv_id = cc.conv_id";
                  sql += " WHERE cc.pagamento_cred_id IN (" + nPagamento + ")";
                  sql += " AND pag.cred_id = " + cred_id;

                  BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }
   }
}
