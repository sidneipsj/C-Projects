using System;
using System.Collections.Generic;
using System.Data;
using System.Text;
using Negocio;
using SQLHelperv2;

namespace DAL
{
   public class DALMensagem
   {   
      //Alterado para SqlServer
      public static bool EnviarMensagem(Mensagens mensagem)
      {
         string sql = "INSERT INTO Mensagens";
         sql += " (MENSAGENS_ID,REMETENTE_TP,REMETENTE_ID,DESTINATARIO_TP,DESTINATARIO_ID,DATAHORA,ASSUNTO,MENSAGEM,LIDO,APAGADO,USUARIO)";
         sql += " VALUES(NEXT VALUE FOR SMENSAGENS_ID,";
         sql += " '" + mensagem.Remetente_tp + "'," + mensagem.Remetente_id + ",";
         sql += " '" + mensagem.Destinatario_tp + "'," + mensagem.Destinatario_id + ",";
         sql += " '" + mensagem.Datahora.ToString("dd/MM/yyyy HH:mm:ss") + "',";
         sql += " '" + mensagem.Assunto + "','" + mensagem.CorpoMensagem + "',";
         sql += " '" + mensagem.Lido + "', '" + mensagem.Apagado + "',";
         sql += " '" + mensagem.Usuario + "')";

         BD BD = new BD();
         if (BD.ExecuteNoQuery(sql, null) == 1)
            return true;
         else return false;
      }

      //Alterado para SqlServer
      public static bool ExisteNaoLidas(string local, int id)
      {
         string sql = "SELECT mensagens_id FROM Mensagens";
         sql += " WHERE apagado = 'N'";
         sql += " AND lido = 'N'";
         if (local.Equals("F"))
            sql += " AND destinatario_tp = 'CREDENCIADOS'";
         else
            sql += " AND destinatario_tp = 'EMPRESAS'";
         sql += " AND destinatario_id = @Id";   
         
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("Id", id));

         BD BD = new BD();
         DataTable resultado = BD.GetDataTable(sql, ps);
         if (resultado.Rows.Count > 0)
            return true;
         else return false;
      }

      public static DataTable GetCaixaEntrada(string local, int id)
      {
         string sql = "SELECT mensagens_id, remetente_tp, datahora, assunto, lido, usuario";
         sql += " FROM Mensagens";
         sql += " WHERE COALESCE(apagado,'N')='N'";
         if(local.Equals("F"))
            sql += " AND destinatario_tp = 'CREDENCIADOS'";
         else
            sql += " AND destinatario_tp = 'EMPRESAS'";
         sql += " AND destinatario_id = @Id";
         sql += " ORDER BY datahora DESC";

         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("Id", id));

         BD BD = new BD();
         return BD.GetDataTable(sql, ps);
      }

      public static DataTable GetItensEnviados(string local, int id)
      {
         string sql = "SELECT mensagens_id, remetente_tp, datahora, assunto, lido, usuario";
         sql += " FROM Mensagens";
         sql += " WHERE COALESCE(apagado,'N')='N'";
         if (local.Equals("F"))
            sql += " AND remetente_tp = 'CREDENCIADOS'";
         else
            sql += " AND remetente_tp = 'EMPRESAS'";
         sql += " AND remetente_id = @Id";
         sql += " ORDER BY datahora DESC";

         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("Id", id));

         BD BD = new BD();
         return BD.GetDataTable(sql, ps);
      }

      public static DataTable GetMensagem(int mensagens_id)
      {
         string sql = "SELECT remetente_tp, remetente_id, datahora, mensagem, assunto, usuario";
         sql += " FROM Mensagens";
         sql += " WHERE mensagens_id = @mensagensId";

         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("mensagensId", mensagens_id));

         BD BD = new BD();
         return BD.GetDataTable(sql, ps);
      }

      public static bool LerMensagem(int mensagens_id)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("mensagensId", mensagens_id));

         MontadorSql mont = new MontadorSql("mensagens", MontadorType.Update);
         mont.AddField("LIDO", "S");
         mont.SetWhere("where MENSAGENS_ID = @mensagensId", ps);

         BD BD = new BD();
         if (BD.ExecuteNoQuery(mont.GetSqlString(),mont.GetParams()) == 1)
            return true;
         else return false;
      }

      public static bool ExcluirMensagem(int mensagens_id)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("mensagensId", mensagens_id));

         MontadorSql mont = new MontadorSql("mensagens", MontadorType.Update);
         mont.AddField("APAGADO", "S");
         mont.SetWhere("where MENSAGENS_ID = @mensagensId", ps);

         BD BD = new BD();
         if (BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams()) == 1)
            return true;
         else return false;
      }    
   }
}
