using System;
using System.Collections.Generic;
using System.Text;
using SQLHelperv2;
using Negocio;
using System.Data;
using Misc;

namespace DAL
{
   public class DALTransacoes
   {
      public static string GetCupom(int trans_id, int cred_id)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("trans", trans_id));
         ps.Add(new Fields("cred", cred_id));

         string sql = "SELECT t.cupom";
         sql += " FROM Transacoes t";
         sql += " WHERE t.trans_id = @trans";
         sql += " AND t.cred_id = @cred";
         sql += " AND t.aberta = 'N'";
         sql += " AND t.confirmada  = 'S'";
         sql += " AND t.cancelado = 'N'";

         try
         {
            BD BD = new BD();
            SafeDataReader dr = BD.GetDataReader(sql, ps);
             string cupom = String.Empty;
             try
             {
                if (dr.Read())
                {
                   byte[] buffer = dr.GetBytes(0);
                   System.Text.Encoding enc = System.Text.Encoding.ASCII;
                   cupom = enc.GetString(buffer);
                }
             }
             finally
             {
                 dr.Close();
             }

            if (cupom.Equals("\0"))
                throw new Exception("Não há cupom gravado para esta transação");
            else
                return cupom;
               
         }
         catch(Exception ex)
         {
            throw new Exception("Erro ao obter cupom: " + ex.Message);
         }
      }

      public static DataTable GetTransacoesDoDia(string operador)
      {
         string sql = " SELECT trans.trans_id, trans.datahora, trans.aberta, trans.cancelado, trans.confirmada, ";
         sql += " conv.conv_id, conv.titular, cart.codigo, cart.digito, cart.codcartimp,";
         sql += " COALESCE(pt.codbarras,'') AS codbarras, COALESCE(pt.descricao,'PRODUTO NAO ENVIADO') AS descricao,";
         sql += " COALESCE(pt.qtd_aprov,0) AS qtd_aprov,";
         sql += " COALESCE(pt.vlr_bru,trans.valor,0.00) AS vlr_bru,";
         sql += " COALESCE(pt.vlr_desc,0.00) AS vlr_desc,";
         sql += " COALESCE(pt.vlr_liq,trans.valor,0.00) AS vlr_liq,";
         sql += " CASE WHEN COALESCE(pt.status,4) = 0 THEN COALESCE(prog.nome,'GRUPO '||gp.descricao)";
         sql += " WHEN COALESCE(pt.status,4) = 1 THEN 'SEM DESCONTO'";
         sql += " WHEN COALESCE(pt.status,4) = 2 THEN 'PRODUTO BLOQUEADO'";
         sql += " WHEN COALESCE(pt.status,4) = 3 THEN 'APLICACAO '||trans.operador";
         sql += " ELSE 'PRODUTO NAO ENVIADO' END tipo_desconto,";
         sql += " COALESCE(cc.autorizacao_id,0) AS autorizacao_id, COALESCE(cc.digito,0) AS digito, COALESCE(cc.debito,0.00) AS debito, COALESCE(cc.historico,'') AS historico";
         sql += " FROM Transacoes trans";
         sql += " LEFT JOIN Prod_Trans pt ON pt.trans_id = trans.trans_id";
         sql += " LEFT JOIN Programas prog ON pt.prog_id = prog.prog_id";
         sql += " LEFT JOIN Grupo_Prod gp ON pt.grupo_prod_id = gp.grupo_prod_id";
         sql += " LEFT JOIN ContaCorrente cc ON cc.trans_id = trans.trans_id";
         sql += " JOIN Cartoes cart ON trans.cartao_id = cart.cartao_id";
         sql += " JOIN Conveniados conv ON cart.conv_id = conv.conv_id";
         sql += " WHERE trans.operador ='" + operador;
         sql += "' AND (trans.datahora BETWEEN '" + DateTime.Now.ToString("MM/dd/yyyy 00:00:00") + "' AND '" + DateTime.Now.ToString("MM/dd/yyyy 23:59:59") + "')";         
         sql += " ORDER BY trans.trans_id";

         BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }


      public static string GetNumTransacao(string autorizacao)
      {
          SqlParamsList ps = new SqlParamsList();
          ps.Add(new Fields("@autor_id", autorizacao));
          BD BD = new BD();
          string s = Convert.ToString(BD.ExecuteScalar("SELECT TRANS_ID FROM AUTOR_TRANSACOES WHERE AUTOR_ID = @autor_id", ps));
          return s;
      }
   }
}
