using System;
using System.Collections.Generic;
using System.Text;
using SQLHelperv2;
using System.Data;
using Negocio;

namespace DAL
{
   public class DALMisc
   {
      public enum TipoLimite
      {
         Mensal,
         Total
      }

      public static TipoLimite GetTipoLimite()
      {
         string sql = "SELECT tipo_limite FROM Config";

         BD BD = new BD();
         string tipo = BD.ExecuteScalar(sql, null).ToString();

         if (tipo.Equals("M"))
            return TipoLimite.Mensal;
         else
            return TipoLimite.Total;         
      }

      public string GetFormasPagto(string forma)
      {
         BD BD = new BD();
         return BD.ExecuteScalar("SELECT descricao FROM Formaspagto WHERE forma_id = " + forma, null).ToString();
      }

      //Alterado para SqlServer
      public static string GetAdministradoraName()
      {
         BD BD = new BD();
         return BD.ExecuteScalar("SELECT fantasia FROM Administradora WHERE apagado <> 'S'", null).ToString();
      }

      public static string GetAdmId()
      {
         BD BD = new BD();
         return BD.ExecuteScalar("SELECT adm_id FROM Administradora WHERE apagado <> 'S'", null).ToString();
      }

      public void DelMov(string mov_id)
      {
         BD BD = new BD();
         BD.ExecuteNoQuery(" DELETE FROM Mov_prod2 WHERE mov_id = " + mov_id, null);
      }

      public static string ContaBoleto()
      {
         BD BD = new BD();
         return BD.ExecuteScalar("SELECT conta_id FROM Administradora", null).ToString();
      }

      public static bool MostraCodigoImportacao()
      {
         /*se todos estiverem vazios ou com N
         vc usará o numero do cartão + digito
         caso o contrário, usará o códcartimp*/

         string sql = "select MOVER_CODCART_TO_CODIMP as VER1, INCREMENTCODCARTIMP as VER2, INCREMENTCODCARTIMPMOD1 as VER3, USAINICIALCODCARTIMP as VER4 from Config";
         BD BD = new BD();
         DataRow config = BD.GetOneRow(sql, null);

         if ((config["VER1"].ToString().Equals("S")) ||
            (config["VER2"].ToString().Equals("S")) ||
            (config["VER3"].ToString().Equals("S")) ||
            (config["VER4"].ToString().Equals("S")))
            return true;

         return false;
      }

      //Alterado para SqlServer
      public static bool Usa_Prog_Desc()
      {
         string sql = "SELECT usa_prog_desc FROM Config";

         BD BD = new BD();
         DataRow config = BD.GetOneRow(sql,null);

         bool usa = false;
         if (config["usa_prog_desc"].Equals("S"))
            usa = true;

         return usa;
      }

      //Alterado para SqlServer
      public static bool Usa_Fidelidade()
      {
         string sql = "SELECT usa_fidelidade FROM Config";

         BD BD = new BD();
         DataRow config = BD.GetOneRow(sql, null);

         bool usa = false;
         if (config["usa_fidelidade"].Equals("S"))
            usa = true;

         return usa;
      }

      public static bool Usa_Vale_Desconto()
      {
         string sql = "SELECT usa_vale_desconto FROM Config";

         BD BD = new BD();
         DataRow config = BD.GetOneRow(sql, null);

         bool usa = false;
         if (config["usa_vale_desconto"].Equals("S"))
            usa = true;

         return usa;
      }

      public static bool Imprime_Cupom_Fidelize()
      {
         string sql = "SELECT imprime_cupom_fidelize FROM Config";

         BD BD = new BD();
         DataRow config = BD.GetOneRow(sql, null);

         bool usa = false;
         if (config["imprime_cupom_fidelize"].Equals("S"))
            usa = true;

         return usa;
      }

      public static bool UsaNovoFechamento()
      {
         BD BD = new BD();
         string usa = BD.ExecuteScalar("SELECT usa_novo_fechamento FROM Config", null).ToString();
         if (usa.Equals("S"))
            return true;
         else return false;
      }

      public static bool Demissao_Move_Auts()
      {
         BD BD = new BD();
         string move = BD.ExecuteScalar("SELECT demissao_move_auts FROM Config", null).ToString();
         if (move.Equals("S"))
            return true;
         else return false;
      }

      public static bool Bloqueia_Venda_Valor()
      {
         BD BD = new BD();
         string bloqueia = BD.ExecuteScalar("SELECT bloqueia_venda_valor FROM Config", null).ToString();
         if (bloqueia.Equals("S"))
            return true;
         else return false;
      }

      public static bool Envia_dados()
      {
         BD BD = new BD();
         string envia = BD.ExecuteScalar("SELECT envia_dados FROM Config", null).ToString();
         if (envia.Equals("S"))
            return true;
         else return false;
      }

      public static UsuariosAdm GetDadosLogin(string usuario)
      {
         string sql = "SELECT usu.usuario_id, usu.nome, usu.liberado AS usuliberado, COALESCE(usu.senha,1111) AS senha,";
         sql += " gru.administrador, gru.liberado AS gruliberado";
         sql += " FROM Usuarios usu";
         sql += " JOIN Grupo_Usuarios gru ON usu.grupo_usu_id = gru.grupo_usu_id";
         sql += " WHERE usu.apagado <> 'S'";
         sql += " AND gru.apagado <> 'S'";
         sql += " AND usu.nome=@nome";

         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("nome", usuario.ToUpper()));

         BD BD = new BD();
         DataRow row = BD.GetOneRow(sql, ps);

         UsuariosAdm usu = new UsuariosAdm();
         
         if(row != null)
         {
            usu.Usuario_id = Convert.ToInt32(row["usuario_id"]);
            usu.Liberado = (!row["usuliberado"].ToString().Equals("N"));
            usu.Senha = row["senha"].ToString();
            usu.Grupo.Administrador = (!row["administrador"].ToString().Equals("N"));
            usu.Grupo.Liberado = (!row["gruliberado"].ToString().Equals("N"));
         }
         else
            usu.Usuario_id = 0;

         return usu;
      }
   }
}
