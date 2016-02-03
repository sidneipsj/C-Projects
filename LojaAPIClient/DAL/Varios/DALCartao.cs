using System;
using System.Collections.Generic;
using System.Text;
using SQLHelperv2;
using netUtil;
using System.Data;
using Negocio;
using Misc;

namespace DAL
{
    //
   public enum TipoCartao { Titular = 0, Dependente = 1 }
   
   public class DALCartao
   {
      public static void incluirCartoesTeste()
      {
        BD BD = new BD();
        try
        {
          BD.ExecuteScalar("execute procedure incluir_cartoes_teste", null);
        }
        catch (Exception e)
        {
          throw new Exception("Erro ao incluir cartões\n O erro ocorrido foi: " + e.Message);
        }
      }

      #region WebEmpresas
      public static string EmpresIdPorCartao(string codcartimp)
      {
        BD BD = new BD();
        SqlParamsList param = new SqlParamsList();
        param.Add(new Fields("codcartimp",codcartimp));
        return BD.ExecuteScalar("","SELECT FIRST 1 CONV.EMPRES_ID FROM CONVENIADOS CONV WHERE CONV.APAGADO <> 'S' and CONV.CONV_ID = (SELECT CARD.CONV_ID FROM CARTOES CARD WHERE CARD.APAGADO <> 'S' AND CARD.CODCARTIMP = @codcartimp)",param);
      }

      //Alterado para SqlServer
      public static bool AtualizarCartao(DataRow alt, DataRow ori, DataTable Cartoes, string operador, int nProtocolo)
      {
         try
         {
            for (int i = 0; i < Cartoes.Columns.Count; i++)
            {
               if (alt[i].ToString().ToUpper() != ori[i].ToString().ToUpper())
               {
                  string campo = Cartoes.Columns[i].ColumnName;
                  string valor = alt[i].ToString().ToUpper();
                  string newval = valor.ToUpper();
                  if (System.Type.GetType("System.String") == Cartoes.Columns[i].DataType)
                    try
                    {
                      newval = "'" + Convert.ToDateTime(valor).ToString("dd.MM.yyyy") + "'";
                    }
                    catch
                    {
                      newval = "'" + valor + "'";
                    }
                  BD BD = new BD();
                  if (BD.ExecuteNoQuery(" UPDATE cartoes SET " + campo + " = " + newval + " , operador = '" + operador + "', dtalteracao = '" + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "' WHERE cartao_id = " + ori["cartao_id"].ToString(), null) == 1)
                  {
                      int logID = Log.GeraLogID();
                      Log.GravaLog(logID, "FCadCartoes", campo, ori[i].ToString(), valor, operador, "Alteração", "Cadastro de Cartões", ori["cartao_id"].ToString(), "Cartão ID: " + ori["cartao_id"].ToString(), "", nProtocolo);
                  }
               }
            }
            return true;
         }
         catch (Exception e)
         {
            string s = e.Message;
            return false;
         }
      }

      public static void DeletarCartoes(string conv_id, string operador)
      {
         BD BD = new BD();
         DataTable cartoes = BD.GetDataTable("select cartao_id from cartoes where apagado <> 'S' and conv_id = " + conv_id, null);

         foreach (DataRow row in cartoes.Rows)
         {            
            int excluiu = BD.ExecuteNoQuery("UPDATE cartoes SET apagado = 'S', liberado = 'N', operador = '" + operador + "', dtapagado = '" +
               DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "', dtalteracao = '" + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "' WHERE cartao_id = " + row["cartao_id"].ToString(), null);

            if (excluiu == 1)
            {
               int logID = Log.GeraLogID();
               //Log.GravaLog(logID,"FCadCartoes", "apagado", "N", "S", operador, "Exclusão", "Cadastro de Cartões", row["cartao_id"].ToString(), "Cartão ID: " + row["cartao_id"].ToString(), "");
               logID = Log.GeraLogID();
               //Log.GravaLog(logID,"FCadCartoes", "dtapagado", "", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), operador, "Exclusão", "Cadastro de Cartões", row["cartao_id"].ToString(), "Cartão ID: " + row["cartao_id"].ToString(), "");               
            }
         }
      }

      //Alterado para SqlServer
      public static bool DeletarCartao(string codigo, string operador, int nProtocolo)
      {
         BD BD = new BD();
         string cartao_id = BD.GetOneRow("select cartao_id from cartoes where codigo=" + codigo, null)["cartao_id"].ToString();

         int excluiu = BD.ExecuteNoQuery("UPDATE cartoes SET apagado = 'S', liberado = 'N', operador = '" + operador + "', dtapagado = '" +
                  DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "', dtalteracao = '" + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "' where cartao_id = " + cartao_id, null);

         if (excluiu == 1)
         {
            int logID = Log.GeraLogID();
            Log.GravaLog(logID,"FCadCartoes", "apagado", "N", "S", operador, "Exclusão", "Cadastro de Cartões", cartao_id, "Cartão ID: " + cartao_id, "", nProtocolo);
            logID = Log.GeraLogID();
            Log.GravaLog(logID,"FCadCartoes", "dtapagado", "", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), operador, "Exclusão", "Cadastro de Cartões", cartao_id, "Cartão ID: " + cartao_id, "", nProtocolo);            
         }

         return (excluiu == 1);
      }

      //Alterado para SqlServer
      public static bool MovimentacoesAberto(string codigo)
      {
          try
          {
              BD BD = new BD();
              string cartao_id = BD.GetOneRow("select cartao_id from cartoes where codigo=" + codigo, null)["cartao_id"].ToString();

              int count = 0;
              using (SafeDataReader dr = BD.GetDataReader("SELECT count(*) FROM contacorrente WHERE COALESCE(baixa_conveniado,'N') = 'N' AND cartao_id = " + cartao_id, null))
              {
                  if ((dr.HasRows()) && (dr.Read()))
                      count = dr.GetInt32(0);
                  dr.Close();
              }
              if (count > 0)
                  return true;
              else return false;
          }
          catch
          {
              return false;
          }
      }

      private static bool INICIALCODCARTIMP()
      {
         BD BD = new BD();
         return (BD.ExecuteScalar("select config.usaINICIALCODCARTIMP from config", null).ToString().ToUpper() == "S");
      }

      //Alterado para SqlServer
      private static bool ConfigMoveCartao()
      {
         BD BD = new BD();
         return (BD.ExecuteScalar("select config.mover_codcart_to_codimp from config", null).ToString().ToUpper() == "S");
      }

      //Alterado para SqlServer
      private bool ConfigIncrementoCartao(TipoCartao cartao)
      {
         BD BD = new BD();
         bool config = (BD.ExecuteScalar("select config.INCREMENTCODCARTIMP from config", null).ToString().ToUpper() == "S");
         if (config)
         {
            if (cartao == 0)
            {
               IncrementoTitular();
            }
            else
            {
               string cartaoDep;               
               cartaoDep = BD.ExecuteScalar("select codigocartaoimpdep from config", null).ToString();
               if (cartaoDep != "")
               {
                  ConfigCodigoIncremento = (Convert.ToInt32(cartaoDep) + 1).ToString();                  
                  BD.ExecuteNoQuery(" update config set codigocartaoimpdep = " + ConfigCodigoIncremento + "", null);
               }
               else
               {
                  IncrementoTitular();
               }
            }
         }
         return config;
      }

      private void IncrementoTitular()
      {
         BD BD = new BD();
         ConfigCodigoIncremento = (Int64.Parse(BD.ExecuteScalar("select codigocartaoimp from config", null).ToString()) + 1).ToString();
         BD.ExecuteNoQuery(" update config set codigocartaoimp = " + ConfigCodigoIncremento + "", null);
      }

      private string _ConfigCodigoIncremento;
      private string ConfigCodigoIncremento
      {
         set { _ConfigCodigoIncremento = value; }
         get { return _ConfigCodigoIncremento; }
      }

      //Alterado para SqlServer
      private static bool ConfigMascaraCartao()
      {
         BD BD = new BD();
         return (BD.ExecuteScalar("select config.INCREMENTCODCARTIMPMOD1 from config", null).ToString().ToUpper() == "S");
      }

      private static string BuscarNumBase()
      {
         BD BD = new BD();
         return (BD.ExecuteScalar("select cod_card_bin from config",null).ToString());
      }

      private static bool codCardTimpExiste(string codCardTimp)
      {
        BD BD = new BD();
        string s = (BD.ExecuteScalar("select coalesce(codcartimp,'') as codcartimp from cartoes where codcartimp = '" + codCardTimp.ToString() + "'", null) == null ? "" : codCardTimp);
        return s.Equals(codCardTimp); 
      }

      //Alterado para SqlServer
      private static bool UsaCodImportacao(int conv_id)
      {
        BD BD = new BD();
        return (BD.ExecuteScalar("select usa_cod_importacao from empresas where empres_id = (select empres_id from conveniados where conv_id = " + conv_id + " and apagado = 'N' and liberado = 'S')", null).ToString().ToUpper() == "S");
      }

      //Alterado para SqlServer
      public static DataTable BuscarCartaoTitular(string convId)
      {
         BD BD = new BD();
         DataTable cartoes = BD.GetDataTable("select ' ' cartao, cartao_id, codigo, digito, limite_mes, liberado, nome, codcartimp from cartoes where titular = 'S' and apagado <> 'S' and conv_id = " + convId, null);
         foreach (DataRow r in cartoes.Rows)
         {
            r["cartao"] = r["codigo"].ToString().PadLeft(9, '0') + "-" + r["digito"].ToString().PadLeft(2, '0');
         }

         return cartoes;
      }

      //Alterado para SqlServer
      public static DataTable BuscarCartoesDependentes(string convId)
      {
         BD BD = new BD();
         DataTable cartoes = BD.GetDataTable("select ' ' cartao, cartao_id, codigo, digito, nome, limite_mes, liberado , titular, num_dep, codcartimp from cartoes where apagado <> 'S' and titular <> 'S' and conv_id = " + convId, null);
         foreach (DataRow r in cartoes.Rows)
         {
            r["cartao"] = r["codigo"].ToString().PadLeft(9, '0') + "-" + r["digito"].ToString().PadLeft(2, '0');
         }

         return cartoes;
      }

      //Alterado para SqlServer
      public static DataTable BuscarCartao(string codigo)
      {
         BD BD = new BD();
         DataTable cartao = BD.GetDataTable("select ' ' cartao, cartao_id, codigo, digito, nome, limite_mes, liberado , titular, num_dep, codcartimp, COALESCE(cred_id,0) AS cred_id, coalesce(data_nasc,'') as data_nasc, ativo, coalesce(jaemitido,'S') jaemitido from cartoes where apagado <> 'S' and codigo = " + codigo, null);
         foreach (DataRow r in cartao.Rows)
         {
            r["cartao"] = r["codigo"].ToString().PadLeft(9, '0') + "-" + r["digito"].ToString().PadLeft(2, '0');
         }

         return cartao;
      }
      #endregion

      #region WebEmpresas, WebEstabelecimentos
      //Alterado para SqlServer
      public static int getConvIdCartao(string codigo, string digito)
      {
        BD BD = new BD();
        int conv_id = -1;
        SafeDataReader dr = BD.GetDataReader("SELECT conv_id FROM cartoes WHERE (codigo = '" + codigo + "' and digito = '" + digito + "') or ( codcartimp = '" + codigo + digito + "')", null);
        try
        {
            if (dr.Read())
            {
                conv_id = dr.GetInt32("conv_id");
            }
        }
        finally
        {
            dr.Close();
        }
        return conv_id;
      }

      public static List<Cartoes> getTodosCartoesConv(int conv_id)
      {
        List<Cartoes> cartoes = new List<Cartoes>();
        BD BD = new BD();
        SqlParamsList parametros = new SqlParamsList();
        parametros.Add(new Fields("conv_id",conv_id));
        string sql = "select\n" +
                     "  card.cartao_id,\n" +
                     "  card.codigo,\n" +
                     "  card.digito,\n" +
                     "  card.codcartimp,\n" +
                     "  card.nome,\n" +
                     "  card.liberado,\n" +
                     "  card.ativo,\n" +
                     "  card.titular\n" +
                     "from\n" +
                     "  cartoes card\n" +
                     "where card.apagado <> 'S'\n" +
                     "and card.conv_id = @conv_id\n";
        using (SafeDataReader dr = BD.GetDataReader(sql, parametros))
        {
          Conveniados c = null;
          while(dr.Read())
          {
            Cartoes card = new Cartoes();
            if (c == null)
            {
              c = DALConveniado.GetConveniado(conv_id);
            }
            card.Conveniado = c;
            card.Cartao_id = dr.GetInt32("cartao_id");
            card.Nome = dr.GetString("nome");
            card.Liberado = dr.GetString("liberado");
            card.Codigo = dr.GetDouble("codigo");
            card.Digito = dr.GetInt32("digito");
            card.Titular = dr.GetString("titular").ToUpper();
            card.Codigodigito = dr.GetString("codigo") + dr.GetString("digito");
            card.CodImp = dr.GetString("codcartimp");
            card.Ativo = dr.GetString("ativo");
            cartoes.Add(card);
          }
          dr.Close();
        }
        return cartoes;
      }

      //Alterado para SqlServer
      public static Cartoes getCartoes(string codigo, string digito)
      {
         Cartoes card = new Cartoes();
         string sql = " select CARTAO_ID, CONV_ID, NOME, LIBERADO, CODIGO, DIGITO, TITULAR, CODCARTIMP, ATIVO  FROM CARTOES" + 
                      " where (codigo = '" + codigo + "' and digito = '" + digito + "') or (codcartimp = '" + codigo + digito + "')";
         BD BD = new BD();
         card.Conveniado.Conv_id = -1;
         card.Cartao_id = -1;
         using (SafeDataReader dr = BD.GetDataReader(sql, null))
         {
            if (dr.Read())
            {
              Conveniados c = DALConveniado.GetConveniado(dr.GetInt32("conv_id"));
              card.Cartao_id = dr.GetInt32("cartao_id");
              card.Conveniado = c;
              card.Nome = dr.GetString("nome");
              card.Liberado = dr.GetString("liberado");
              card.Codigo = Convert.ToDouble(dr.GetObject("codigo"));
              card.Digito = Convert.ToInt32(dr.GetObject("digito"));
              card.Titular = dr.GetString("titular").ToUpper();
              card.Codigodigito = Convert.ToString(dr.GetObject("codigo")) + Convert.ToString(dr.GetObject("digito"));
              card.CodImp = dr.GetString("codcartimp");
              card.Ativo = dr.GetString("ativo");
            }
            dr.Close();
         }
         return card;
      }

      //Alterado para SqlServer
      public static string GetDtNascimentoCartao(string codigo, string digito)
      {
        BD BD = new BD();
        try
        {
          string s = Convert.ToDateTime(BD.ExecuteScalar("SELECT coalesce(data_nasc,'') as data_nasc FROM cartoes WHERE (cartoes.codigo = '" + codigo + "' and digito = '" + digito + "') or ( codcartimp = '" + codigo + digito + "')", null)).ToString("dd/MM/yyyy");
          return s;
        }
        catch (Exception e)
        {
          return string.Empty;
        }
      }

      //Alterado para SqlServer
      public int GetNumVia(string codigo, string digito)
      {
        BD BD = new BD();
        return Convert.ToInt32(BD.ExecuteScalar("SELECT via FROM cartoes WHERE (codigo = '" + codigo + "' and digito = '" + digito + "') or ( codcartimp = '" + codigo + digito + "')",null).ToString());
      }

      //Alterado para SqlServer
      public Cartoes gerarNovaVia(string codigo, string digito, int nProtocolo, bool apenasAlteracao = false)
      {
        BD BD = new BD();
        int via = 0;
        string titular = string.Empty;
        string codCartImp = string.Empty;
        Cartoes card = getCartoes(codigo, digito);
        #region Gerando CODCARTIMP
        
        if (card.Conveniado.Conv_id != -1 && card.Cartao_id != -1)
        {
          bool naoGerarCartaoMenor = DALEmpresa.GetNaoGerarCartaoParaMenor(card.Conveniado.Empresa.Empres_id);
          string dtNascimento = GetDtNascimentoCartao(codigo, digito);
          bool maiorIdade = false;
          via = GetNumVia(codigo, digito);
          if (dtNascimento != "")
          {
            maiorIdade = DALCartao.MaiorDeIdade(Convert.ToDateTime(dtNascimento));
          }
          bool encontrado;
          if ((naoGerarCartaoMenor && maiorIdade) || (!naoGerarCartaoMenor) || card.CodImp != "")  // Se eu não puder gerar para menor e for maior de idade eu gero! Se eu puder gerar cartão para menor de idade eu gero. Se eu já tiver um cartão eu gero.
          {
            TipoCartao cartao = titular == "S" ? TipoCartao.Titular : TipoCartao.Dependente;
            Cartoes c = new Cartoes();
            if (UsaCodImportacao(card.Conveniado.Conv_id))
            {
              do
              {
                string nb = BuscarNumBase();
                if (nb.Equals("") || nb.Length < 6)
                {
                  card.Cartao_id = -2;
                  return card;
                }
                codCartImp = c.geraCartaoCom8primeirosDig(Int32.Parse(BuscarNumBase()));
                encontrado = codCardTimpExiste(codCartImp);
              } while (encontrado);
            }
            else if (INICIALCODCARTIMP())
            {
                int i = (int)BD.ExecuteScalar("SELECT NEXT VALUE FOR SINICODCARTIMP", null);
              string a = BD.ExecuteScalar("select INICIALCODCARTIMP from config ", null).ToString();
              codCartImp = a + i.ToString("00000000");
            }
            else if (ConfigMoveCartao())
            {
                codCartImp = Convert.ToDouble(BD.ExecuteScalar("SELECT NEXT VALUE FOR SCARTAO_NUM ", null)).ToString();
            }
            else if (ConfigIncrementoCartao(cartao))
            {
              codCartImp = ConfigCodigoIncremento;
            }
            else if (ConfigMascaraCartao())
            {
              Conveniados conv = DALConveniado.GetConveniado(card.Conveniado.Conv_id);
              int seq = ((int)BD.ExecuteScalar("select coalesce(count(*),1) as total from cartoes where conv_id = " + card.Conveniado.Conv_id, null) + 1);
              codCartImp = (conv.Empresa.Empres_id.ToString("0000") + conv.Chapa.ToString("000000") + seq.ToString("00"));
            }
          }
          else
          {
            card.Cartao_id = -3; //para dizer que o usuário do cartão é menor de idade...
          }
        }
        #endregion
        if (codCartImp.Trim() != null && card.Cartao_id != -1 && card.Conveniado.Conv_id != -1)
        {
          card.CodImp = codCartImp;
          via = (apenasAlteracao ? via : via + 1 );
          if (BD.ExecuteNoQuery("UPDATE CARTOES SET CODCARTIMP = '" + codCartImp + "', JAEMITIDO = 'N', VIA = " + via.ToString() + " WHERE CARTAO_ID = " + card.Cartao_id, null) == 1)
          {
              int logID = Log.GeraLogID();
              Log.GravaLog(logID, "FCadCartoes", "cartao_id", "", card.Cartao_id.ToString(), "WS " + card.Cartao_id.ToString(), "Alteração", "Cadastro de Cartões", card.Cartao_id.ToString(), "Cartão ID: " + card.Cartao_id.ToString(), "", nProtocolo);
          }
        }
        return card;
      }

      //Alterado para SqlServer
      public static bool MaiorDeIdade(DateTime DataNascimento)
      {
        TimeSpan date = Convert.ToDateTime(DateTime.Now) - DataNascimento;

        int totalAnos = date.Days / 365;
        return totalAnos >= 18;
      }

      //Alterado para SqlServer
      public Cartoes IncluiCartao(int conv_id, string nome, double limite_mes, string liberado, string operador, string titular, int num_dep, int cred_id, string ativo, string DtNascimento, string codigoImportacao, int nProtocolo)
      {
         BD BD = new BD();
         int cartao_id = Convert.ToInt32(BD.ExecuteScalar("SELECT NEXT VALUE FOR SCARTAO_ID AS CARTAO_ID", null).ToString());          
         TipoCartao cartao = titular == "S" ? TipoCartao.Titular : TipoCartao.Dependente;
         string codCartImp = string.Empty;
         double codcartao = Convert.ToDouble(BD.ExecuteScalar("SELECT NEXT VALUE FOR SCARTAO_NUM ", null));
         int empres_id = DALConveniado.GetConvEmpresID(conv_id);
         bool naoGeraCartaoParaMenor = DALEmpresa.GetNaoGerarCartaoParaMenor(empres_id);
         bool maiorDeIdade = (!DtNascimento.Equals("")? MaiorDeIdade(Convert.ToDateTime(DtNascimento)):false);
         string jaEmitido = (titular == "N"?"S":"N");
         if (((naoGeraCartaoParaMenor && maiorDeIdade) || (!naoGeraCartaoParaMenor)) || titular == "S")
         {
           jaEmitido = "N";
           if (!string.IsNullOrEmpty(codigoImportacao))
             codCartImp = codigoImportacao;
           else
           {
             bool encontrado;
             Cartoes c = new Cartoes();
             if (UsaCodImportacao(conv_id))
             {
               do
               {
                 codCartImp = c.geraCartaoCom8primeirosDig(Int32.Parse(BuscarNumBase()));
                 encontrado = codCardTimpExiste(codCartImp);
               } while (encontrado);
             }
             else if (INICIALCODCARTIMP())
             {
               int i = (int)BD.ExecuteScalar("SELECT NEXT VALUE FOR SINICODCARTIMP", null);
               string a = BD.ExecuteScalar("select INICIALCODCARTIMP from config ", null).ToString();
               codCartImp = a + i.ToString("00000000");
             }
             else if (ConfigMoveCartao())
             {
               codCartImp = codcartao.ToString();
             }
             else if (ConfigIncrementoCartao(cartao))
             {
               codCartImp = ConfigCodigoIncremento;
             }
             else if (ConfigMascaraCartao())
             {
               Conveniados conv = DALConveniado.GetConveniado(conv_id);
               int seq = ((int)BD.ExecuteScalar("select coalesce(count(*),1) as total from cartoes where conv_id = " + conv_id, null) + 1);
               codCartImp = (conv.Empresa.Empres_id.ToString("0000") + conv.Chapa.ToString("000000") + seq.ToString("00"));
             }
           }
         }
         netUtil.Funcoes func = new netUtil.Funcoes();
         int digito = func.DigitoCartao(codcartao);

         MontadorSql mont = new MontadorSql("cartoes", MontadorType.Insert);
         mont.AddField("cartao_id", cartao_id);
         mont.AddField("codigo", codcartao);
         mont.AddField("digito", digito);
         mont.AddField("conv_id", conv_id);
         mont.AddField("nome", Utils.TirarAcento(nome.ToUpper()));
         mont.AddField("limite_mes", limite_mes);
         mont.AddField("dtcadastro", System.DateTime.Now);
         mont.AddField("liberado", liberado);
         mont.AddField("operador", operador);
         mont.AddField("jaemitido", jaEmitido);
         mont.AddField("apagado", 'N');
         mont.AddField("titular", titular.ToUpper());
         if (!DtNascimento.Equals(""))
           mont.AddField("data_nasc", DtNascimento);
         mont.AddField("num_dep", (num_dep > 0 ? num_dep : 0));
         if (((naoGeraCartaoParaMenor && maiorDeIdade) || (!naoGeraCartaoParaMenor)) || titular == "S")
         {
             mont.AddField("codcartimp", codCartImp == string.Empty ? null : codCartImp);
         }
         if (cred_id != 0)
            mont.AddField("cred_id", cred_id);
         mont.AddField("ativo", ativo);
         mont.AddField("empres_id", empres_id);

         Cartoes cart = new Cartoes();

         if (BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams()) == 1)
         {
            int logID = Log.GeraLogID();
            Log.GravaLog(logID, "FCadCartoes", "cartao_id", "", cartao_id.ToString(), operador, "Inclusão", "Cadastro de Cartões", cartao_id.ToString(), "Cartão ID: " + cartao_id.ToString(), "", nProtocolo);

            cart.Cartao_id = cartao_id;
            cart.Codigo = codcartao;
            cart.Digito = digito;
            cart.Codigodigito = cart.Codigo.ToString() + cart.Digito.ToString("00");
            cart.CodImp = codCartImp;
         }
         else cart.Cartao_id = 0;

         return cart;
      }

      //Alterado para SqlServer
      public static bool ExisteCartaoCadastrado(string codimportacao)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("codimportacao", codimportacao));

         string sql = "SELECT cartao_id FROM Cartoes WHERE codcartimp = @codimportacao AND apagado <> 'S'";

         BD BD = new BD();
         DataTable table = BD.GetDataTable(sql, ps);

         return table.Rows.Count > 0;
      }
      
      #endregion

      #region WebUsuarios
      //Alterado para SqlServer
      public static DataTable BuscarTodosCartoes(int convId)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("conv_id", convId));
         string sql = " select nome, case when liberado = 'S' then 'Sim' else 'Não' end liberado, " +
                      " codcartimp, case when titular = 'S' then 'Sim' else 'Não' end titular, " +
                      " parentesco, data_nasc, ' ' cartao, codigo, digito " +
                      " from cartoes where apagado <> 'S' and conv_id = @conv_id";

         BD BD = new BD();
         DataTable dt = BD.GetDataTable(sql, ps);

         foreach (DataRow r in dt.Rows)
         {
            r["cartao"] = r["codigo"].ToString().PadLeft(9, '0') + r["digito"].ToString().PadLeft(2, '0');
         }

         return dt;
      }
      #endregion

      #region CANTINEX

      public static DataTable BuscarTodosCartoesCantinex(int convId)
      {
          SqlParamsList ps = new SqlParamsList();
          ps.Add(new Fields("conv_id", convId));
          string sql = "SELECT CART.NOME, CART.CODCARTIMP, CART.LIMITE_MES, CART.LIMITE_DIARIO, "
                     + "SUM(COALESCE(CART.LIMITE_MES, 0) - COALESCE(CART.CONSUMO_ATUAL, 0)) AS SALDORESTANTE, "
                     + "CART.LIBERADO FROM CARTOES CART "
                     + "WHERE CART.TITULAR <> 'S' AND CART.CONV_ID = " + convId + " "
                     + "GROUP BY CART.NOME, CART.CODCARTIMP, CART.LIMITE_MES, CART.LIMITE_DIARIO, CART.LIBERADO";

          BD BD = new BD();
          DataTable dt = BD.GetDataTable(sql, ps);

          return dt;
      }

      public static DataTable BuscarFilhosCartoesCantinex(int convId)
      {
          SqlParamsList ps = new SqlParamsList();
          ps.Add(new Fields("conv_id", convId));
          string sql = "SELECT '<TODOS>' AS NOME, '0' AS CODCARTIMP FROM CARTOES UNION SELECT NOME, CODCARTIMP FROM CARTOES WHERE CODCARTIMP <> '' AND CONV_ID = " + convId + " ORDER BY NOME";

          BD BD = new BD();
          DataTable dt = BD.GetDataTable(sql, ps);

          return dt;
      }

      public static void AlterarStatusCartaoCantinex(string liberado, string cartaoId)
      {
          BD BD = new BD();
          if (liberado == "S")
          {
              BD.ExecuteNoQuery("UPDATE CARTOES SET LIBERADO = 'N' WHERE CODCARTIMP = '" + cartaoId + "'", null);
          }
          else
          {
              BD.ExecuteNoQuery("UPDATE CARTOES SET LIBERADO = 'S' WHERE CODCARTIMP = '" + cartaoId + "'", null);
          }
      }

      public static void AlterarLimiteDiarioCantinex(string valor, string cartaoId)
      {
          BD BD = new BD();
          BD.ExecuteNoQuery("UPDATE CARTOES SET LIMITE_DIARIO = " + valor + " WHERE CODCARTIMP = '" + cartaoId + "'", null);
      }

      public static DataTable CartoesCantinexParaCreditos(int convId)
      {
          SqlParamsList ps = new SqlParamsList();
          ps.Add(new Fields("conv_id", convId));
          string sql = "SELECT CART.NOME, CONV.TITULAR, CART.CODCARTIMP, CONV.ENDERECO, CONV.BAIRRO, "
                     + "CONV.CIDADE, CONV.CEP, CONV.ESTADO, CONV.CPF, 0.00 AS SALDO, CONV.EMAIL, CART.CARTAO_ID FROM CARTOES CART "
                     + "INNER JOIN CONVENIADOS CONV ON(CONV.CONV_ID = CART.CONV_ID) WHERE CART.TITULAR <> 'S' AND CART.CONV_ID = @conv_id";

          BD BD = new BD();
          DataTable dt = BD.GetDataTable(sql, ps);
          return dt;
      }

      public static int SequenceCantinaCredito()
      {
          string sql = "SELECT NEXT VALUE FOR SCANTINA_CREDITOS";
          BD BD = new BD();
          int valor = Convert.ToInt16(BD.ExecuteScalar(sql, null));

          return valor;
      }

      public static bool CantinaCredito(int creditoId, int convId, decimal vCredito, decimal vTaxa, string operador, DateTime dataHora)
      {
          MontadorSql mont = new MontadorSql("CANTINA_CREDITOS", MontadorType.Insert);
          mont.AddField("CREDITO_ID", creditoId);
          mont.AddField("CONV_ID", convId);
          mont.AddField("VALOR_CREDITO", vCredito);
          mont.AddField("VALOR_TAXA", vTaxa);
          mont.AddField("OPERADOR", operador);
          mont.AddField("DATA_HORA", dataHora);

          BD BD = new BD();
          if (BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams()).Equals(1))
          {
              return true;
          }
          else
              return false;
      }

      public static bool CantinaCreditosCartoes(int creditoId, int cartaoId, decimal valor, char enviouRemessa)
      {
          MontadorSql mont = new MontadorSql("CANTINA_CREDITOS_CARTOES", MontadorType.Insert);
          mont.AddField("CREDITO_ID", creditoId);
          mont.AddField("CARTAO_ID", cartaoId);
          mont.AddField("VALOR", valor);
          mont.AddField("ENVIOU_REMESSA", enviouRemessa);

          BD BD = new BD();
          if (BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams()).Equals(1))
          {
              return true;
          }
          else
              return false;
      }

      public static DataTable CarregarCreditosCantinex(int convId, DateTime periodo)
      {
          SqlParamsList ps = new SqlParamsList();
          ps.Add(new Fields("conv_id", convId));
          string sql = "";

          BD BD = new BD();
          DataTable dt = BD.GetDataTable(sql, ps);
          return dt;
      }

      public static bool VerificaCreditoDia(int convId, string periodo, string credito)
      {
          string sql = "SELECT VALOR_CREDITO FROM CANTINA_CREDITOS WHERE CONV_ID = " + convId + " AND DATA_HORA BETWEEN '" + periodo + " 00:00:00' AND '" + periodo + " 23:59:59' AND VALOR_CREDITO = '" + credito + "'";
          BD BD = new BD();
          DataTable b = BD.GetDataTable(sql, null);
          if (b.Rows.Count > 0)
          {
              return true;
          }
          else
          {
              return false;
          }
      }

      public static bool VerificaCreditoMes(int convId, DateTime periodoInicial, DateTime periodoFinal)
      {
          string sql = "SELECT VALOR_CREDITO FROM CANTINA_CREDITOS WHERE CONV_ID = " + convId + " AND DATA_HORA BETWEEN '" + periodoInicial + "' AND '" + periodoFinal + "'";
          BD BD = new BD();
          DataTable b = BD.GetDataTable(sql, null);
          if (b.Rows.Count > 0)
          {
              return true;
          }
          else
          {
              return false;
          }
      }


      public static DataTable GetCreditosDiarios()
      {
          string sql = "SELECT CART.CONV_ID, CONVERT(DATE,CURRENT_TIMESTAMP) as DATA, CART.CODCARTIMP, CART.LIBERADO, CART.APAGADO,"
              + " COALESCE(CART.LIMITE_MES,0) AS LIMITE_MES, COALESCE(CART.LIMITE_DIARIO,0) AS LIMITE_DIARIO,"
              + " COALESCE(CART.CONSUMO_ATUAL,0) AS CONSUMO_ATUAL"
              + " FROM CARTOES CART"
              + " JOIN EMPRESAS EMP ON EMP.EMPRES_ID = CART.EMPRES_ID"
              + " WHERE EMP.TIPO_CREDITO = 4";

          BD BD = new BD();
          DataTable dt = BD.GetDataTable(sql, null);

          return dt;
      }


      #endregion
   }
}
