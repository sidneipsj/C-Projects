using System;
using System.Collections.Generic;
using System.Text;
using Negocio;
using System.Data;
using SQLHelperv2;
using Misc;
using netUtil;

namespace DAL
{
   public class DALConveniado
   {

	  public static void incluirConveniadosTeste()
	  {
		BD BD = new BD();
		try
		{
		  BD.ExecuteScalar("execute procedure incluir_conveniados_teste", null);
		}
		catch (Exception e)
		{
		  throw new Exception("Ocorreu um erro ao incluir os conveniados teste.\n O erro ocorrido foi: " + e.Message);
		}
	  }
	  
	  #region WebEstabelecimentos
	  public static double GetRestante(int pConveniadoId, int pEmpresaId, int pLimite, int bandID)
	  {
		 SqlParamsList ps = new SqlParamsList();
		 ps.Add(new Fields("@convId", pConveniadoId));
		 BD BD = new BD();

		  if(bandID  != 999)
		  {
			  return Convert.ToDouble(BD.ExecuteScalar(" select coalesce(consumo_mes_" + pLimite + ",0) from conveniados where conv_id = @convId ", ps));
		  }
		  else
		  {
			  return Convert.ToDouble(BD.ExecuteScalar(" select (coalesce(limite_mes,0) + coalesce(abono_mes,0) + coalesce(saldo_acumulado,0)) - coalesce(consumo_mes_1,0) from conveniados where conv_id = @convId", ps));
		  }
	  }


	  //Alterado para SqlServer
	  public bool ConsCartao(string cartao, Conveniados conv)
	  {
		 string sql = " SELECT CASE cart.liberado WHEN 'S' THEN 'Sim' ELSE 'Não' END AS cartlib, cart.nome AS cnome,";
		 sql += " conv.conv_id, conv.titular, CASE conv.liberado WHEN 'S' THEN 'Sim' ELSE 'Não' END AS convlib,";
		 sql += " conv.limite_mes, CASE empr.liberada WHEN 'S' THEN 'Sim' ELSE 'Não' END AS emplib, Empr.NOME empnome,";
		 sql += " conv.obs1, conv.email, conv.obs2, conv.fidelidade AS convfidelidade, empr.fidelidade AS empfidelidade, empr.band_id, empr.empres_id ";
		 sql += " FROM Cartoes cart, Conveniados conv, Empresas empr";
		 sql += " WHERE conv.empres_id = empr.empres_id";
		 sql += " AND cart.conv_id = conv.conv_id ";
		 sql += " AND cart.apagado <> 'S' AND empr.apagado <> 'S' AND conv.apagado <> 'S'";

		 if (cartao.Length > 3)
		 {
			string codigo = cartao.Substring(0, cartao.Length - 2);
			string digito = cartao.Substring(cartao.Length - 2, 2);

			sql += " AND(( Cart.codigo = " + codigo;
			sql += " AND Cart.digito = " + digito + " )";
			sql += " OR (Cart.codcartimp = '" + cartao + "'))";
		 }
		 else
			sql += " AND Cart.codcartimp ='" + cartao + "'";

		 BD BD = new BD();
		 SafeDataReader dr = BD.GetDataReader(sql, null);
		 bool retorno = false;
		 try
		 {
			 if (dr.Read())
			 {
				 Cartoes cart = new Cartoes();
				 cart.Liberado = dr.GetString("cartlib");
				 cart.Nome = dr.GetString("cnome");
				 conv.addCartao(cart);

				 conv.Empresa = new Empresas();
				 conv.Empresa.Liberada = dr.GetString("emplib");
				 conv.Empresa.Nome = dr.GetString("empnome");
				 conv.Conv_id = dr.GetInt32("conv_id");
				 conv.Status = dr.GetString("convlib");
				 conv.Titular = dr.GetString("titular");
				 conv.LimiteMes = Convert.ToSingle(dr.GetDecimal("limite_mes"));
				 conv.Obs1 = dr.GetString("obs1");
				 conv.Obs2 = dr.GetString("obs2");
				 conv.Email = dr.GetString("email");
				 conv.Empresa.Bandeira.Band_id = dr.GetInt32("band_id");
				 conv.Empresa.Empres_id = dr.GetInt32("empres_id");

				 string convfidelidade = dr.GetString("convfidelidade");
				 string empfidelidade = dr.GetString("empfidelidade");

				 if ((convfidelidade.Equals("S")) || (empfidelidade.Equals("S")))
					 conv.Fidelidade = "Sim";
				 else
					 conv.Fidelidade = "Não";

				 if (conv.Fidelidade.Equals("Sim"))
					 conv.Saldo_pontos = GetSaldoPontosConveniado(conv.Conv_id);

				 retorno = true;
			 }
		 }
		 finally
		 {
			 dr.Close();
		 }

		 return retorno;
	  }

	  //Alterado para SqlServer
	  public DataTable GetSaldoDisponivel(int convId, int bandID = 999, int credId = 0, int empres_id = 0)
	  {
			BD BD = new BD();

			string sql = "";
			bool salDif = false;
			if (bandID != 999)
			{
				TDbAutor autor = new TDbAutor();
				int codLimite = autor.GetCodLimite(bandID, credId);
				salDif = DALConveniado.GetUsaSaldoDif(convId);

				if (salDif.Equals(false))
				{
					if (codLimite == 1)
						sql = " select coalesce(b.limite_1,0) as limite_mes, coalesce(conv.consumo_mes_1,0) as saldo_mes, (b.limite_1 - coalesce(conv.consumo_mes_1,0)) ";
					else if (codLimite == 2)
						sql = " select coalesce(b.limite_2,0) as limite_mes, coalesce(conv.consumo_mes_2,0) as saldo_mes, (b.limite_2 - coalesce(conv.consumo_mes_2,0)) ";
					else if (codLimite == 3)
						sql = " select coalesce(b.limite_3,0) as limite_mes, coalesce(conv.consumo_mes_3,0) as saldo_mes, (b.limite_3 - coalesce(conv.consumo_mes_3,0)) ";
					else if (codLimite == 4)
						sql = " select coalesce(b.limite_4,0) as limite_mes, coalesce(conv.consumo_mes_4,0) as saldo_mes, (b.limite_4 - coalesce(conv.consumo_mes_4,0)) ";
				}
				else
				{
					if (codLimite == 1)
						sql = " select coalesce(bc.limite_1,0) as limite_mes, coalesce(conv.consumo_mes_1,0) as saldo_mes, (bc.limite_1 - coalesce(conv.consumo_mes_1,0)) ";
					else if (codLimite == 2)
						sql = " select coalesce(bc.limite_2,0) as limite_mes, coalesce(conv.consumo_mes_2,0) as saldo_mes, (bc.limite_2 - coalesce(conv.consumo_mes_2,0)) ";
					else if (codLimite == 3)
						sql = " select coalesce(bc.limite_3,0) as limite_mes, coalesce(conv.consumo_mes_3,0) as saldo_mes, (bc.limite_3 - coalesce(conv.consumo_mes_3,0)) ";
					else if (codLimite == 4)
						sql = " select coalesce(bc.limite_4,0) as limite_mes, coalesce(conv.consumo_mes_4,0) as saldo_mes, (bc.limite_4 - coalesce(conv.consumo_mes_4,0)) ";
				}
			}
			else
			{
				sql = " select coalesce(conv.limite_mes,0) as limite_mes, coalesce(conv.consumo_mes_1,0) as saldo_mes, ";
				sql += "(conv.limite_mes + case when e.tipo_credito in (2,3) then coalesce(conv.saldo_acumulado,0) + coalesce(conv.abono_mes,0) else 0 end) -";
				sql += " coalesce(conv.consumo_mes_1,0) ";
			}
			sql += " as restante, ";
			sql += " (select TOP 1 dia_fecha.data_fecha from dia_fecha where dia_fecha.data_fecha > current_timestamp and dia_fecha.empres_id = " + empres_id + " order by 1) as fechamento";
			sql += " from conveniados conv";
			sql += " join empresas e on e.empres_id = conv.empres_id";
			if (bandID != 999)
			{
				if (salDif.Equals(false))
				{
					sql += " join bandeiras b on b.band_id = e.band_id";
				}
				else
				{
					sql += " join bandeiras_conv bc on bc.conv_id = conv.conv_id";
				}
			}
			sql += " where conv.conv_id = " + convId;
			 
			return BD.GetDataTable(sql, null); 

	  }


	  public DataTable GetSaldoConv(int conv_id)
	  {
		 BD BD = new BD();
		 return BD.GetDataTable(" SELECT saldo_conv.* FROM saldo_conv(" + conv_id + ") ORDER BY fechamento ", null);
	  }

	  //Alterado para SqlServer
	  public static DataTable GetLogConveniado(string protocolo)
	  {
		  BD BD = new BD();
		  return BD.GetDataTable(" SELECT * FROM LOGS WHERE PROTOCOLO = " + protocolo, null);
	  }

	  public static Conveniados GetConveniado(string cartao, string cpf, bool verifica_inc_cart_pbm, bool pendente)
	  {
		 string sql = " SELECT conv.conv_id, conv.titular, conv.cpf, conv.rg, conv.telefone1, conv.email, ";
		 sql += " conv.endereco, conv.numero, conv.bairro, conv.cidade, conv.estado, conv.cep, conv.fidelidade AS convfidelidade, ";
		 sql += " cart.nome, cart.codigo, cart.digito, cart.codcartimp, cart.ativo, ";
		 sql += " emp.empres_id, emp.nome AS nomeempr, emp.fidelidade AS empfidelidade,";
		 sql += " conv.obs1, conv.obs2, conv.limite_mes, conv.cargo, conv.setor";
		 sql += " FROM Conveniados conv";
		 sql += " JOIN Cartoes cart ON conv.conv_id = cart.conv_id";
		 sql += " JOIN Empresas emp ON conv.empres_id = emp.empres_id";
		 sql += " WHERE cart.apagado <> 'S' AND conv.apagado <> 'S' AND emp.apagado <> 'S'";
		 if (verifica_inc_cart_pbm)
			sql += " AND emp.inc_cart_pbm <> 'N'";
		 if (pendente)
			sql += " AND cart.ativo <> 'S'";
		 if (cpf != "")
		 {
			sql += " AND (conv.cpf = '" + cpf + "' OR conv.cpf = '" + cpf.Replace(".", "").Replace("-", "") + "')";
			sql += " AND cart.titular = 'S'";
		 }
		 else
		 {
			string codigo = "";
			string digito = "";
			if (cartao.Length > 3)
			{
			   codigo = cartao.Substring(0, cartao.Length - 2);
			   digito = cartao.Substring(cartao.Length - 2, 2);

			   sql += " AND(( cart.codigo = " + codigo;
			   sql += " AND cart.digito = " + digito + " )";
			   sql += " OR (cart.codcartimp = '" + cartao + "'))";
			}
			else sql += " AND cart.codcartimp = '" + cartao + "'";
		 }

		 BD BD = new BD();
		 DataTable dt = BD.GetDataTable(sql, null);

		 Conveniados conv = new Conveniados();

		 if (dt.Rows.Count > 0)
		 {
			DataRow row = dt.Rows[0];

			conv.Conv_id = Convert.ToInt32(row["conv_id"]);
			conv.Titular = row["titular"].ToString();
			conv.Cpf = row["cpf"].ToString();
			conv.Rg = row["rg"].ToString();
			conv.Telefone1 = row["telefone1"].ToString();
			conv.LimiteMes = float.Parse(row["limite_mes"].ToString());
			conv.Endereco.Logradouro = row["endereco"].ToString();
			try
			{
			   conv.Endereco.Numero = Convert.ToInt32(row["numero"]);
			}
			catch
			{
			   conv.Endereco.Numero = 0;
			}
			conv.Endereco.Bairro = row["bairro"].ToString();
			conv.Endereco.Cidade = row["cidade"].ToString();
			conv.Endereco.Uf = row["estado"].ToString();
			conv.Endereco.Cep = row["cep"].ToString();
			Cartoes cart = new Cartoes();
			cart.Nome = row["nome"].ToString();
			cart.Codigo = Convert.ToDouble(row["codigo"]);
			cart.Digito = Convert.ToInt32(row["digito"]);
			cart.Codigodigito = cart.Codigo.ToString() + cart.Digito.ToString("00");
			cart.CodImp = row["codcartimp"].ToString();
			cart.Ativo = row["ativo"].ToString();
			conv.addCartao(cart);            
			conv.Empresa.Empres_id = Convert.ToInt32(row["empres_id"]);
			conv.Empresa.Nome = row["nomeempr"].ToString();

			string convfidelidade = row["convfidelidade"].ToString();
			string empfidelidade = row["empfidelidade"].ToString();
			conv.Fidelidade = "N";
			if ((convfidelidade.Equals("S")) || (empfidelidade.Equals("S")))
			   conv.Fidelidade = "S";
			if (conv.Fidelidade.Equals("S"))
			   conv.Saldo_pontos = GetSaldoPontosConveniado(conv.Conv_id);
			conv.Email = row["email"].ToString();
			conv.Obs1 = row["obs1"].ToString();
			conv.Obs2 = row["obs2"].ToString();
			conv.Cargo = row["cargo"].ToString();
			conv.Setor = row["setor"].ToString();
		 }

		 return conv;
	  }

	  public static int GetSaldoPontosConveniado(int conv_id)
	  {
		 string sql = " SELECT COALESCE(SUM(credito-debito),0) AS saldo_pontos";
		 sql += " FROM Fidel_Historico";
		 sql += " WHERE conv_id = " + conv_id;
		 sql += " AND cancelado <> 'S'";
		 sql += " AND ((dataexpira >= '" + DateTime.Now.ToString("MM/dd/yyyy") + "') OR (dataexpira IS NULL))";

		 BD BD = new BD();
		 int saldo_pontos = (int)BD.ExecuteScalar(sql, null);

		 return saldo_pontos;
	  }
	  #endregion

	  #region WebEmpresas

	  //Alterado para SqlServer
	  public static DataTable GetParentesco(int conv_id)
	  {
		  BD BD = new BD();
		  return BD.GetDataTable(" SELECT nome, parentesco FROM cartoes where conv_id = " + conv_id + " and titular = 'N'  and liberado <> 'N' and apagado <> 'S' ORDER BY nome ", null);
	  }

	  public static DataTable GetConvsEmp(int empresId)
	  {
		  BD BD = new BD();
		  SqlParamsList pl = new SqlParamsList();
		  pl.Add(new Fields("empres_id",empresId));

		  string sql =  "select\n" + 
						"  c.conv_id,\n" +
						"  c.titular,\n" + 
						"  card.codcartimp,\n" + 
						"  e.nome empresa\n" +
						"from conveniados c\n" +
						"join cartoes card on card.conv_id = c.conv_id\n" +
						"join empresas e on e.empres_id = c.empres_id\n" +
						"where c.empres_id = @empres_id\n" +
						"and coalesce(card.titular,'S') = 'S'\n" +
						"and card.apagado <> 'S'\n" + 
						"and c.apagado <> 'S'\n" +
						"and e.apagado <> 'S'\n" +
						"order by titular\n";


		  return BD.GetDataTable(sql, pl); 
	  }

	  //Alterado para SqlServer
	  public static String GerarProtocolo()
	  {
		BD BD = new BD();
		return BD.ExecuteScalar(-1, "SELECT NEXT VALUE FOR SPROTOCOLO", null).ToString(); 
	  }

	  //Alterado para SqlServer
	  public static DataTable GetSetores(int empres_id)
	  {
		  BD BD = new BD();
		  string sql = "SELECT DEPT_ID,DESCRICAO FROM EMP_DPTOS where DESCRICAO is not null and DPTO_APAGADO = 'N' and empres_id = " + empres_id.ToString() + " ORDER BY DESCRICAO";
		  return BD.GetDataTable(sql, null);
	  }

	  //Alterado para SqlServer
	  public static int GetEmpresBandId(int empres_id)
	  {
		 Conveniados conv = new Conveniados();
		 string sql = " SELECT band_id FROM empresas where empres_id = " + empres_id.ToString();
		 BD BD = new BD();
		 using (SafeDataReader dr = BD.GetDataReader(sql, null))
		 {
		   if (dr.Read())
		   {
			 return dr.GetInt32("band_id");
		   } 
		   else
			 return 999;
		 }
	  }

	  //Alterado para SqlServer
	  public static string GetDescricaoLimite(int band_id, int limite)
	  {
		Conveniados conv = new Conveniados();
		string sql = " SELECT desc_limite_" + limite.ToString() + " FROM bandeiras where band_id = " + band_id.ToString();
		BD BD = new BD();
		using (SafeDataReader dr = BD.GetDataReader(sql, null))
		{
		  if (dr.Read())
		  {
			return dr.GetString("desc_limite_" + limite.ToString());
		  }
		  else
			return "";
		}
	  }

	  //Alterado para SqlServer
	  public static bool GetUsaSaldoDif(int conv_id)
	  {
		string sql = "SELECT usa_saldo_dif FROM conveniados where conv_id = " + conv_id.ToString();
		BD BD = new BD();
		using (SafeDataReader dr = BD.GetDataReader(sql, null))
		{
		  if (dr.Read())
		  {
			return dr.GetString("usa_saldo_dif") == "S";
		  }
		  else
			return false;
		}
	  }

	  //Alterado para SqlServer
	  public static bool GetUsaSaldoDif(string cartao)
	  {
		
		if (cartao.Equals("")) 
		  return false;
		else
		{
		  string codigo = cartao.Substring(0, cartao.Length - 2);
		  string digito = cartao.Substring(cartao.Length - 2, 2);
		  string sql = "SELECT usa_saldo_dif FROM conveniados where conv_id = (SELECT conv_id FROM cartoes card where (card.codigo = " + codigo + " and card.digito = " + digito + ") or (card.codcartimp = " + cartao + "))";
		  BD BD = new BD();
		  using (SafeDataReader dr = BD.GetDataReader(sql, null))
		  {
			if (dr.Read())
			{
			  return dr.GetString("usa_saldo_dif") == "S";
			}
			else
			  return false;
		  }
		}
	  }

	  //Alterado para SqlServer
	  public static DataTable PesquisaPaginada(int page, int empres_id, string chapa, string cartao, string nome, string status, string grupo)
	  {
		 int band_id = GetEmpresBandId(empres_id);

		 string sql;

		 sql = " DECLARE @TamanhoPagina INT; ";
		 sql += " DECLARE @NumeroPagina INT; ";
		 sql += " SET @TamanhoPagina = 10; ";
		 sql += " SET @NumeroPagina = " + page + ";";
		 sql += " WITH Paginado AS ( ";
		 sql += " SELECT ROW_NUMBER() OVER(ORDER BY CONV.TITULAR) AS linha, ";
		 sql += "        conv.conv_id,";
		 sql += "        conv.chapa, \n";
		 sql += "        conv.titular, \n";
		 sql += "        (case conv.liberado when 'S' then 'Liberado' else 'Bloqueado' end) as status, \n";
		 sql += "        case \n";
		 sql += "          when emp.band_id <> 999 then \n";
		 sql += "           (case \n";
		 sql += "          when bc.conv_id = conv.conv_id then \n";
		 sql += "           bc.limite_1 \n";
		 sql += "          else \n";
		 sql += "           b.limite_1 \n";
		 sql += "        end) else conv.limite_mes end as limite_mes, \n";
		 sql += "        coalesce(case \n";
		 sql += "                   when bc.conv_id = conv.conv_id then \n";
		 sql += "                     bc.limite_2 \n";
		 sql += "                   else \n";
		 sql += "                     b.limite_2 \n";
		 sql += "                 end, \n";
		 sql += "                 0) as limite_mes_2, \n";
		 sql += "  \n";
		 sql += "        coalesce(case \n";
		 sql += "                   when bc.conv_id = conv.conv_id then \n";
		 sql += "                    bc.limite_3 \n";
		 sql += "                   else \n";
		 sql += "                    b.limite_3 \n";
		 sql += "                 end, \n";
		 sql += "                 0) as limite_mes_3, \n";
		 sql += "        coalesce(case \n";
		 sql += "                   when bc.conv_id = conv.conv_id then \n";
		 sql += "                    bc.limite_4 \n";
		 sql += "                   else \n";
		 sql += "                    b.limite_4 \n";
		 sql += "                 end, \n";
		 sql += "                 0) as limite_mes_4, \n";
		 sql += "        COALESCE((case \n";
		 sql += "                   when emp.band_id <> 999 then \n";
		 sql += "                    (case \n";
		 sql += "                   when bc.conv_id = conv.conv_id then \n";
		 sql += "                    bc.limite_1 \n";
		 sql += "                   else \n";
		 sql += "                    b.limite_1 \n";
		 sql += "                 end) ";
		 sql += "                 else (case when emp.tipo_credito = 2 or emp.tipo_credito = 3 then (conv.limite_mes + conv.saldo_acumulado + conv.abono_mes) \n";
		 sql += "          		  else conv.limite_mes end) end) - \n";
		 sql += "        coalesce(conv.consumo_mes_1,0), 0) as saldo_disponivel_1, \n";
		 sql += "        coalesce((case \n";
		 sql += "                   when emp.band_id <> 999 then \n";
		 sql += "                    (case \n";
		 sql += "                   when bc.conv_id = conv.conv_id then \n";
		 sql += "                    bc.limite_2 \n";
		 sql += "                   else \n";
		 sql += "                   b.limite_2 \n";
		 sql += "                 end) else 0 end) - \n";
		 sql += "        coalesce(conv.consumo_mes_2,0), 0) as saldo_disponivel_2, \n";
		 sql += "        COALESCE((case \n";
		 sql += "                   when emp.band_id <> 999 then \n";
		 sql += "                    (case \n";
		 sql += "                   when bc.conv_id = conv.conv_id then \n";
		 sql += "                    bc.limite_3 \n";
		 sql += "                   else \n";
		 sql += "                    b.limite_3 \n";
		 sql += "                 end) else 0 end) - \n";
		 sql += "        coalesce(conv.consumo_mes_3,0), 0) as saldo_disponivel_3, \n";
		 sql += "        coalesce((case \n";
		 sql += "                   when emp.band_id <> 999 then \n";
		 sql += "                    (case \n";
		 sql += "                   when bc.conv_id = conv.conv_id then \n";
		 sql += "                    bc.limite_4 \n";
		 sql += "                   else \n";
		 sql += "                    b.limite_4 \n";
		 sql += "                 end) else 0 end) - \n";
		 sql += "        coalesce(conv.consumo_mes_4,0), 0) as saldo_disponivel_4, \n";
		 sql += "   coalesce(conv.saldo_acumulado,0) as acumulado, \n";
		 sql += "   coalesce(conv.consumo_mes_1,0) + coalesce(conv.consumo_mes_2,0) + coalesce(conv.consumo_mes_3,0) + coalesce(conv.consumo_mes_4,0) as saldo_total, \n";
		 sql += "   coalesce(conv.consumo_mes_1,0) + coalesce(conv.consumo_mes_2,0) + coalesce(conv.consumo_mes_3,0) + coalesce(conv.consumo_mes_4,0) as saldo_prox \n";
		 sql += "   FROM conveniados conv \n";
		 sql += "   JOIN Cartoes cart ON cart.conv_id = conv.conv_id \n";
		 sql += "   join empresas emp on conv.empres_id = emp.empres_id \n";
		 sql += "   join bandeiras b on b.band_id = emp.band_id \n";
		 sql += "   left join bandeiras_conv bc on conv.conv_id = bc.conv_id \n";
	  
		 string where = " WHERE conv.apagado <> 'S' AND cart.apagado <> 'S' AND cart.liberado <> 'I' AND conv.empres_id = " + empres_id;

		 if (grupo != "0")
			where += " AND conv.grupo_conv_emp=" + grupo;

		 netUtil.Funcoes funcoes = new netUtil.Funcoes();

		 if (chapa != "")
			where += " AND conv.chapa = " + chapa;
		 else
			if (cartao != "") //procura pelo cartao
			{
			   string codigo = "";
			   string digito = "";
			   if (cartao.Length > 3)
			   {
				 codigo = cartao.Substring(0, cartao.Length - 2);
				 digito = cartao.Substring(cartao.Length - 2, 2);

				 where += " AND(( cart.codigo = " + codigo;
				 where += " AND cart.digito = " + digito + " )";
				 where += " OR (cart.codcartimp = '" + cartao + "'))";
			   }
			   else
				 where += " AND cart.codcartimp ='" + cartao + "'";
			}
			else
			   if (nome != "")//procura pelo Nome
				  where += " AND (conv.titular LIKE '" + (nome.Length > 3 ? "%" : "") + nome.ToUpper() + "%')";

		 if (!status.Equals("T"))
			where += " AND conv.liberado = '" + status + "'";
		 string group = " group by conv.titular, conv.conv_id, conv.CHAPA, conv.LIBERADO, EMP.BAND_ID, CONV.limite_mes, BC.CONV_ID, BC.LIMITE_1, B.LIMITE_1, "
				+ " BC.LIMITE_2, B.LIMITE_2, BC.LIMITE_3, B.LIMITE_3, BC.LIMITE_4, B.LIMITE_4, CONV.CONSUMO_MES_1, CONV.CONSUMO_MES_2, conv.CONSUMO_MES_3, "
				+ " CONV.CONSUMO_MES_4, CONV.SALDO_ACUMULADO, emp.tipo_credito,conv.abono_mes ";
		 string paginado = ") SELECT TOP (@TamanhoPagina) * FROM Paginado p WHERE linha > @TamanhoPagina * (@NumeroPagina - 1)  ORDER BY titular ";

		 BD BD = new BD();
		 return BD.GetDataTable(sql + where + group + paginado , null);
	  }

	  public static DataTable GetSaldoRestante(int empres_id, int conv_id)
	  {
		  string sql = " SELECT  distinct conv.conv_id,"
				+ " coalesce((case"
				+ " when emp.band_id <> 999 then"
				+ " (case"
				+ " when bc.conv_id = conv.conv_id then"
				+ " bc.limite_1"
				+ " else"
				+ " b.limite_1"
				+ " end) else conv.limite_mes end) -"
				+ " coalesce(spc.saldo_mes,0), 0) as saldo_disponivel_1,"
				+ " coalesce((case" 
				+ " when emp.band_id <> 999 then" 
				+ " (case" 
				+ " when bc.conv_id = conv.conv_id then" 
				+ " bc.limite_2" 
				+ " else" 
				+ " b.limite_2" 
				+ " end) else conv.limite_mes end) -" 
				+ " coalesce(spc.saldo_mes_2,0), 0) as saldo_disponivel_2," 
				+ " COALESCE((case" 
				+ " when emp.band_id <> 999 then" 
				+ " (case" 
				+ " when bc.conv_id = conv.conv_id then" 
				+ " bc.limite_3" 
				+ " else" 
				+ " b.limite_3" 
				+ " end) else conv.limite_mes end) -" 
				+ " coalesce(spc.saldo_mes_3,0), 0) as saldo_disponivel_3," 
				+ " coalesce((case" 
				+ " when emp.band_id <> 999 then" 
				+ " (case" 
				+ " when bc.conv_id = conv.conv_id then" 
				+ " bc.limite_4" 
				+ " else" 
				+ " b.limite_4" 
				+ " end) else conv.limite_mes end) -" 
				+ " coalesce(spc.saldo_mes_4,0), 0) as saldo_disponivel_4"
				+ " FROM conveniados conv" 
				+ " join empresas emp on conv.empres_id = emp.empres_id" 
				+ " join bandeiras b on b.band_id = emp.band_id" 
				+ " left join bandeiras_conv bc on conv.conv_id = bc.conv_id" 
				+ " left join saldo_proxfecha_conv(conv.conv_id) as spc on spc.conv_id = conv.conv_id"
				+ " WHERE conv.apagado <> 'S'"
				+ " AND conv.empres_id = " + empres_id + " AND conv.conv_id = " + conv_id;
		  BD BD = new BD();
		  return BD.GetDataTable(sql, null);
	  }

	  public static int PesqPagVirtualItemCount(int empres_id, string chapa, string cartao, string nome, string status, string grupo)
	  {
		 string where = " WHERE conv.apagado <> 'S' AND cart.apagado <> 'S' AND conv.empres_id = " + empres_id;

		 if (grupo != "0")
			where += " AND conv.grupo_conv_emp=" + grupo;

		 netUtil.Funcoes funcoes = new netUtil.Funcoes();

		 if (chapa != "")
			where += " AND conv.chapa = " + chapa;
		 else
			if (cartao != "") //procura pelo cartao
			{
			   string codigo = "";
			   string digito = "";
			   if (cartao.Length > 3)
			   {
				  codigo = cartao.Substring(0, cartao.Length - 2);
				  digito = cartao.Substring(cartao.Length - 2, 2);

				  where += " AND(( cart.codigo = " + codigo;
				  where += " AND cart.digito = " + digito + " )";
				  where += " OR (cart.codcartimp = '" + cartao + "'))";
			   }
			   else
				  where += " AND cart.codcartimp ='" + cartao + "'";
			}
			else
			   if (nome != "")//procura pelo Nome
				  where += " AND (conv.titular LIKE '" + (nome.Length > 3 ? "%" : "") + nome.ToUpper() + "%')";

		 if (!status.Equals("T"))
			where += " AND conv.liberado = '" + status + "'";

		 BD BD = new BD();
		 return BD.ExecuteScalar<Int32>(0, " SELECT DISTINCT count(*) FROM Conveniados conv JOIN Cartoes cart ON conv.conv_id = cart.conv_id and cart.titular = 'S' " + where, null);
	  }



	  public static DataTable PesquisaTotal(int empres_id, string chapa, string cartao, string nome, string status, string grupo, bool total, bool empr_prog_desc, DALMisc.TipoLimite tipo_limite)
	  {
		 string sql = "SELECT DISTINCT conv.chapa, conv.titular,";
		 sql += " (CASE conv.liberado WHEN 'S' THEN 'Liberado' ELSE 'Bloqueado' END) AS status ";
		 if (!empr_prog_desc)
		 {
			sql += " ,conv.limite_mes";
			if(tipo_limite != DALMisc.TipoLimite.Total)
			   sql += " ,COALESCE((SELECT saldo_mes FROM saldo_conv(conv.conv_id) WHERE fechamento = (SELECT Min(data_fecha) FROM dia_fecha WHERE data_fecha >= current_timestamp AND empres_id = Conv.Empres_id)),0) AS saldo_prox";            
			if (total)
			   sql += ", COALESCE((SELECT SUM(saldo_mes) FROM saldo_conv(conv.conv_id)),0) AS saldo_total";
		 }
		 sql += " FROM Conveniados conv";
		 sql += " JOIN Cartoes cart ON conv.conv_id = cart.conv_id";

		 string where = " WHERE conv.apagado <> 'S' AND cart.apagado <> 'S' AND conv.empres_id = " + empres_id;

		 if (grupo != "0")
			where += " AND conv.grupo_conv_emp=" + grupo;

		 netUtil.Funcoes funcoes = new netUtil.Funcoes();

		 if (chapa != "")
			where += " AND conv.chapa = " + chapa;
		 else
			if (cartao != "") //procura pelo cartao
			{
			   string codigo = "";
			   string digito = "";
			   if (cartao.Length > 3)
			   {
				  codigo = cartao.Substring(0, cartao.Length - 2);
				  digito = cartao.Substring(cartao.Length - 2, 2);

				  sql += " AND(( cart.codigo = " + codigo;
				  sql += " AND cart.digito = " + digito + " )";
				  sql += " OR (cart.codcartimp = '" + cartao + "'))";
			   }
			   else
				  sql += " AND cart.codcartimp ='" + cartao + "'";
			}
			else
			   if (nome != "")//procura pelo Nome
				  where += " AND (conv.titular LIKE '" + (nome.Length > 3 ? "%" : "") + nome.ToUpper() + "%')";

		 if (!status.Equals("T"))
			where += " AND conv.liberado = '" + status + "'";

		 BD BD = new BD();
		 return BD.GetDataTable(sql + where + " ORDER BY conv.titular ", null);
	  }

	  //Alterado para SqlServer
	  public static Conveniados IncluiConv(Conveniados pConv, string pOperador, int cred_id, string cartao_ativo, string codigoImportacao, Bandeiras_Conv band_conv, int nProtocolo)
	  {
		 pConv.Conv_id = GeraConvId();

		 if (pConv.Chapa == 0)
			pConv.Chapa = pConv.Conv_id;

		 MontadorSql mont = new MontadorSql("conveniados", MontadorType.Insert);
		 mont.AddField("conv_id", pConv.Conv_id);
		 mont.AddField("chapa", pConv.Chapa); //Matricula
		 mont.AddField("titular", Utils.TirarAcento(pConv.Titular.ToUpper()));
		 mont.AddField("liberado", pConv.Status.ToUpper());
		 mont.AddField("contrato", Convert.ToInt32(pConv.Conv_id));
		 mont.AddField("empres_id", pConv.Empresa.Empres_id);
		 netUtil.Funcoes func = new netUtil.Funcoes();
		 mont.AddField("senha", func.Crypt("E", "1111", "BIGCOMPRAS"));
		 mont.AddField("dtcadastro", System.DateTime.Now);
		 mont.AddField("banco", 0);
		 mont.AddField("apagado", 'N');
		 mont.AddField("endereco", Utils.TirarAcento(pConv.Endereco.Logradouro.ToUpper()));
		 mont.AddField("numero", pConv.Endereco.Numero);
		 mont.AddField("bairro", Utils.TirarAcento(pConv.Endereco.Bairro.ToUpper()));
		 mont.AddField("cidade", Utils.TirarAcento(pConv.Endereco.Cidade.ToUpper()));
		 mont.AddField("estado", pConv.Endereco.Uf);
		 mont.AddField("cep", pConv.Endereco.Cep.ToUpper());
		 mont.AddField("operador", pOperador);
		 mont.AddField("cpf", pConv.Cpf);
		 mont.AddField("rg", pConv.Rg.ToUpper());
		 mont.AddField("obs1", Utils.TirarAcento(pConv.Obs1.ToUpper()));
		 mont.AddField("obs2", Utils.TirarAcento(pConv.Obs2.ToUpper()));
		 mont.AddField("telefone1", pConv.Telefone1.ToUpper());
		 mont.AddField("email", pConv.Email);
		 mont.AddField("cargo", Utils.TirarAcento(pConv.Cargo.ToUpper()));
		 mont.AddField("setor", Utils.TirarAcento(pConv.Setor.ToUpper()));
		 mont.AddField("usa_saldo_dif", pConv.Usa_saldo_dif.ToUpper());
		 int incluiu;
		 if (pConv.Usa_saldo_dif.Equals("S") && band_conv != null)
		 {
		   MontadorSql mont2 = new MontadorSql("bandeiras_conv", MontadorType.Insert);
		   mont2.AddField("conv_id", band_conv.Conv_id);
		   if (band_conv.Limite_1 > 0)
			 mont2.AddField("limite_1", band_conv.Limite_1);
		   if (band_conv.Limite_2 > 0)
			 mont2.AddField("limite_2", band_conv.Limite_2);
		   if (band_conv.Limite_3 > 0)
			 mont2.AddField("limite_3", band_conv.Limite_3);
		   if (band_conv.Limite_4 > 0)
			 mont2.AddField("limite_4", band_conv.Limite_4);
		   mont.AddField("limite_mes", band_conv.Limite_1 + band_conv.Limite_2 + band_conv.Limite_3 + band_conv.Limite_4);
		   BD BD2 = new BD();
		   incluiu = BD2.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());
		   if (incluiu == 1)
		   {
			 //Log.GravaLog("FCadConv", "conv_id", "", pConv.Conv_id.ToString(), pOperador, "Inclusão", "Cadastro de bandeira de Conveniados", pConv.Conv_id.ToString(), "Conv ID: " + pConv.Conv_id, "");
		   }
		   else
		   {
			 pConv.Conv_id = 0;
			 return pConv;
		   }             
		 }
		 else
		   mont.AddField("limite_mes", pConv.LimiteMes);
		 if (pConv.Grupo_conv_emp != 0)
			mont.AddField("grupo_conv_emp", pConv.Grupo_conv_emp);
		 if (!string.IsNullOrEmpty(pConv.DtNasc))
			mont.AddField("dt_nascimento", pConv.DtNasc);
		 BD BD = new BD();
		 incluiu = BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams());
		 if (incluiu == 1)  //se entrar cria o cartão
		 {
			int logID = Log.GeraLogID();
			Log.GravaLog(logID, "FCadConv", "conv_id", "", pConv.Conv_id.ToString(), pOperador, "Inclusão", "Cadastro de Conveniados", pConv.Conv_id.ToString(), "Conv ID: " + pConv.Conv_id, "", nProtocolo);
			Cartoes cart = new DALCartao().IncluiCartao(pConv.Conv_id, Utils.TirarAcento(pConv.Titular), pConv.LimiteMes, pConv.Status, pOperador, "S", 0, cred_id, cartao_ativo, pConv.DtNasc, codigoImportacao, nProtocolo);
			pConv.addCartao(cart);
		 }
		 else pConv.Conv_id = 0;

		 return pConv;
	  }
	  public static float SomaLimitesBand_Conv(Bandeiras_Conv bc)
	  {
		return bc.Limite_1 + bc.Limite_2 + bc.Limite_3 + bc.Limite_4;
	  }

	  public static int GetConvEmpresID(int conv_id)
	  {
		BD BD = new BD();
		return Convert.ToInt32(BD.ExecuteScalar("SELECT empres_id FROM conveniados where conv_id = " + conv_id.ToString(), null).ToString());
	  }

	  //Alterado para SqlServer
	  public static void AtualizarTitular(string conv_id, string titular)
	  {
		  BD BD = new BD();
		  Convert.ToInt32(BD.ExecuteScalar("UPDATE CARTOES SET NOME = '" + titular + "' WHERE CONV_ID = " + conv_id + " AND TITULAR = 'S'", null));
	  }

	  //Alterado para SqlServer
	  public static void AtualizarConveniado(Conveniados Alt, Conveniados Ori, string operador, Bandeiras_Conv bc, int nProtocolo)
	  {
		 try
		 {
			string id = Ori.Conv_id.ToString();
			if (Alt.Grupo_conv_emp != Ori.Grupo_conv_emp)
				ConfirmAlteracao("grupo_conv_emp", Alt.Grupo_conv_emp, id, Ori.Grupo_conv_emp, operador, nProtocolo);
			if (Alt.Chapa != Ori.Chapa)
				ConfirmAlteracao("chapa", Alt.Chapa, id, Ori.Chapa, operador, nProtocolo);
			if (Alt.Titular.ToUpper() != Ori.Titular.ToUpper())
			{
				ConfirmAlteracao("titular", Utils.TirarAcento(Alt.Titular).ToUpper(), id, Ori.Titular.ToUpper(), operador, nProtocolo);
				AtualizarTitular(id, Utils.TirarAcento(Alt.Titular).ToUpper());     
			}
			if (Alt.Rg != Ori.Rg)
				ConfirmAlteracao("rg", Alt.Rg, id, Ori.Rg, operador, nProtocolo);
			if (Alt.Cpf != Ori.Cpf)
				ConfirmAlteracao("cpf", Alt.Cpf, id, Ori.Cpf, operador, nProtocolo);
			if (Alt.Setor.ToUpper() != Ori.Setor.ToUpper())
				ConfirmAlteracao("setor", Utils.TirarAcento(Alt.Setor).ToUpper(), id, Ori.Setor.ToUpper(), operador, nProtocolo);
			if (Alt.Cargo.ToUpper() != Ori.Cargo.ToUpper())
				ConfirmAlteracao("cargo", Utils.TirarAcento(Alt.Cargo).ToUpper(), id, Ori.Cargo.ToUpper(), operador, nProtocolo);
			if (bc == null)
			{
			  if (Alt.LimiteMes != Ori.LimiteMes)
				  ConfirmAlteracao("limite_mes", Alt.LimiteMes, id, Ori.LimiteMes, operador, nProtocolo);
			}
			else
			{
			  float somaLimite = SomaLimitesBand_Conv(bc);
			  if (Alt.LimiteMes != somaLimite)
				  ConfirmAlteracao("limite_mes", somaLimite, id, Ori.LimiteMes, operador, nProtocolo);
			}
			if (Alt.Endereco.Logradouro.ToUpper() != Ori.Endereco.Logradouro.ToUpper())
				ConfirmAlteracao("endereco", Utils.TirarAcento(Alt.Endereco.Logradouro).ToUpper(), id, Ori.Endereco.Logradouro.ToUpper() ?? "", operador, nProtocolo);
			if (Alt.Endereco.Numero != Ori.Endereco.Numero)
				ConfirmAlteracao("numero", Alt.Endereco.Numero, id, Ori.Endereco.Numero.ToString() ?? "", operador, nProtocolo);
			if (Alt.Endereco.Bairro.ToUpper() != Ori.Endereco.Bairro.ToUpper())
				ConfirmAlteracao("bairro", Utils.TirarAcento(Alt.Endereco.Bairro).ToUpper(), id, Ori.Endereco.Bairro.ToUpper(), operador, nProtocolo);
			if (Alt.Endereco.Cep != Ori.Endereco.Cep)
				ConfirmAlteracao("cep", Alt.Endereco.Cep, id, Ori.Endereco.Cep, operador, nProtocolo);
			if (Alt.Endereco.Cidade.ToUpper() != Ori.Endereco.Cidade.ToUpper())
				ConfirmAlteracao("cidade", Utils.TirarAcento(Alt.Endereco.Cidade).ToUpper(), id, Ori.Endereco.Cidade.ToUpper(), operador, nProtocolo);
			if (Alt.Endereco.Uf != Ori.Endereco.Uf)
				ConfirmAlteracao("estado", Alt.Endereco.Uf, id, Ori.Endereco.Uf, operador, nProtocolo);
			if (Alt.Telefone1 != Ori.Telefone1)
				ConfirmAlteracao("telefone1", Alt.Telefone1, id, Ori.Telefone1, operador, nProtocolo);
			if (Alt.Obs1.ToUpper() != Ori.Obs1.ToUpper())
				ConfirmAlteracao("obs1", Utils.TirarAcento(Alt.Obs1).ToUpper(), id, Ori.Obs1.ToUpper(), operador, nProtocolo);
			if (Alt.Obs2.ToUpper() != Ori.Obs2.ToUpper())
				ConfirmAlteracao("obs2", Utils.TirarAcento(Alt.Obs2).ToUpper(), id, Ori.Obs2.ToUpper(), operador, nProtocolo);
			if (Alt.DtNasc != Ori.DtNasc)
			{
			   if ((Ori.DtNasc == "01/01/0001") || (Ori.DtNasc == ""))
				  Ori.DtNasc = null;
			   if ((Alt.DtNasc == "01/01/0001") || (Alt.DtNasc == ""))
				  Alt.DtNasc = null;

			   if (!((Ori.DtNasc == null) && (Alt.DtNasc == null)))
				   ConfirmAlteracao("dt_nascimento", Alt.DtNasc, id, Ori.DtNasc, operador, nProtocolo);
			}
			if (Alt.Email != Ori.Email)
				ConfirmAlteracao("email", Alt.Email, id, Ori.Email, operador, nProtocolo);
			if (Alt.Usa_saldo_dif != Ori.Usa_saldo_dif)
				ConfirmAlteracao("usa_saldo_dif", Alt.Usa_saldo_dif.ToUpper(), id, Ori.Usa_saldo_dif.ToUpper(), operador, nProtocolo);
			if (!Alt.Status.Equals(Ori.Status))
				ConfirmAlteracao("liberado", (Alt.Status.ToUpper().Equals("S") ? "S" : "N"), id, Ori.Status.ToUpper(), operador, nProtocolo);
		 }
		 catch (Exception e)
		 {
			throw new Exception("Erro ao atualizar conveniado: " + e.Message);
		 }
	  }

	  //Alterado para SqlServer
	  public static bool IncluirBandeira_conv(Bandeiras_Conv band_conv, string pOperador, int nProtocolo)
	  {
		MontadorSql mont2 = new MontadorSql("bandeiras_conv", MontadorType.Insert);
		mont2.AddField("conv_id", band_conv.Conv_id);
		if (band_conv.Limite_1 > 0)
		  mont2.AddField("limite_1", band_conv.Limite_1);
		if (band_conv.Limite_2 > 0)
		  mont2.AddField("limite_2", band_conv.Limite_2);
		if (band_conv.Limite_3 > 0)
		  mont2.AddField("limite_3", band_conv.Limite_3);
		if (band_conv.Limite_4 > 0)
		  mont2.AddField("limite_4", band_conv.Limite_4);
		BD BD2 = new BD();
		int incluiu = BD2.ExecuteNoQuery(mont2.GetSqlString(), mont2.GetParams());
		if (incluiu == 1)
		{
		  int logID = Log.GeraLogID();
		  Log.GravaLog(logID,"FCadConv", "conv_id", "", band_conv.Conv_id.ToString(), pOperador, "Inclusão", "Cadastro de Bandeira do Conveniados", band_conv.Conv_id.ToString(), "Conv ID: " + band_conv.Conv_id, "", nProtocolo);
		  return true;
		}
		else
		  return false;
	  }
	  
	  public static void ApagarBandeiras_Conv(int conv_id, bool apagar = true)
	  {
		BD BD = new BD();
		Convert.ToInt32(BD.ExecuteScalar(" UPDATE bandeiras_conv set APAGADO = '" + (apagar==true?"S":"N") + "' WHERE conv_id = " + conv_id.ToString(),null));
	  }

	  //Alterado para SqlServer
	  public static void AtualizarBandeiras_Conv(Bandeiras_Conv Alt, Bandeiras_Conv Ori, string operador, int nProtocolo)
	  {
		try
		{
		  string id = Ori.Conv_id.ToString();
		  if (Alt.Limite_1 != Ori.Limite_1)
			  ConfirmAlteracaoBandConv("limite_1", Alt.Limite_1, id, Ori, operador, nProtocolo);
		  if (Alt.Limite_2 != Ori.Limite_2)
			  ConfirmAlteracaoBandConv("limite_2", Alt.Limite_2, id, Ori, operador, nProtocolo);
		  if (Alt.Limite_3 != Ori.Limite_3)
			  ConfirmAlteracaoBandConv("limite_3", Alt.Limite_3, id, Ori, operador, nProtocolo);
		  if (Alt.Limite_4 != Ori.Limite_4)
			  ConfirmAlteracaoBandConv("limite_4", Alt.Limite_4, id, Ori, operador, nProtocolo);
		}
		catch (Exception e)
		{
		  throw new Exception("Erro ao atualizar limite do conveniado: " + e.Message);
		}
	  }

	  //Alterado para SqlServer
	  private static void ConfirmAlteracao(string campo, object newval, string conv_id, object oldval, string operador, int nProtocolo)
	  {
		 MontadorSql mont = new MontadorSql("conveniados", MontadorType.Update);
		 mont.AddField(campo, newval);
		 mont.AddField("dtalteracao", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"));
		 mont.AddField("operador", operador);
		 mont.SetWhere("WHERE conv_id = " + conv_id, null);

		 BD BD = new BD();
		 if (BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams()) == 1)
		 {
			if (newval == null) newval = "";
			if (oldval == null) oldval = "";
			int logID = Log.GeraLogID();
			Log.GravaLog(logID,"FCadConv", campo, oldval.ToString(), newval.ToString(), operador, "Alteração", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, "", nProtocolo);
		 }
	  }

	  //Alterado para SqlServer
	  private static void ConfirmAlteracaoBandConv(string campo, object newval, string conv_id, object oldval, string operador, int nProtocolo)
	  {
		MontadorSql mont = new MontadorSql("bandeiras_conv", MontadorType.Update);
		mont.AddField(campo, newval);
		mont.AddField("dtalteracao", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"));
		mont.AddField("operador", operador);
		mont.SetWhere("WHERE conv_id = " + conv_id, null);

		BD BD = new BD();
		if (BD.ExecuteNoQuery(mont.GetSqlString(), mont.GetParams()) == 1)
		{
		  if (newval == null) newval = "";
		  if (oldval == null) oldval = "";
		  int logID = Log.GeraLogID();
		  Log.GravaLog(logID,"FCadConv", campo, oldval.ToString(), newval.ToString(), operador, "Alteração", "Cadastro de Bandeira Diferenciada do Conveniado", conv_id, "Conv ID: " + conv_id, "", nProtocolo);
		}
	  }

	  public static bool MovimentacoesAberto(string conv_id)
	  {
		 int count = 0;
		 BD BD = new BD();
		 SafeDataReader dr = BD.GetDataReader(
			" SELECT COUNT(*) FROM contacorrente WHERE COALESCE(baixa_conveniado,'N') = 'N' AND conv_id = " + conv_id, null);
		 try
		 {
			 if ((dr.HasRows()) && (dr.Read()))
				 count = dr.GetInt32(0);
		 }
		 finally
		 {
			 dr.Close();
		 }
		 if (count > 0)
			return true;
		 else return false;
	  }

	  public static bool DeletaConveniado(string conv_id, string operador)
	  {
		 double chapaExcluida; //novo valor para chapa (negativo pela exclusão)
		 BD BD = new BD();
		 chapaExcluida = Convert.ToDouble(BD.ExecuteScalar("SELECT MIN(chapa) FROM conveniados WHERE apagado = 'S' AND chapa < 0", null));
		 chapaExcluida = chapaExcluida - 1;

		 DALCartao.DeletarCartoes(conv_id, operador);
		 
		 int excluiu = BD.ExecuteNoQuery("UPDATE conveniados SET chapa=" + chapaExcluida +
						   ", apagado = 'S', liberado = 'N', operador='" + operador + "', dtapagado = '" +
						   DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "', dtalteracao = '" + DateTime.Now.ToString("MM/dd/yyyy HH:mm:ss") + "' WHERE conv_id = " + conv_id, null);

		 if (excluiu == 1)
		 {
			int logID = Log.GeraLogID();
			//Log.GravaLog(logID,"FCadConv", "apagado", "N", "S", operador, "Exclusão", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, "");
			logID = Log.GeraLogID();
			//Log.GravaLog(logID,"FCadConv", "dtapagado", "", DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss"), operador, "Exclusão", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, "");            
		 }

		 return (excluiu == 1);
	  }

	  //Alterado para SqlServer
	  public static int GeraConvId()
	  {
		 BD BD = new BD();
		 return Convert.ToInt32(BD.ExecuteScalar(" SELECT NEXT VALUE FOR SCONV_ID AS CONV_ID ", null).ToString());
	  }

	  //Alterado para SqlServer
	  public static Conveniados GetConveniado(int pConvId)
	  {
		 Conveniados conv = new Conveniados();
		 string sql = " SELECT conv.conv_id, emp.empres_id, emp.prog_desc, conv.chapa, conv.titular, conv.dt_nascimento, conv.cpf, " +
					  " conv.rg, conv.email, conv.endereco, COALESCE(conv.numero,0) AS resnumero, conv.bairro, conv.cep, conv.cidade, conv.estado, conv.telefone1, " +
					  " conv.cargo, conv.setor, conv.liberado, conv.limite_mes, conv.obs1, conv.obs2, conv.grupo_conv_emp, conv.usa_saldo_dif " +
					  //" conv.consumo_mes_1, consumo_mes_2, consumo_mes_3, consumo_mes_4 " +
					  " FROM conveniados conv JOIN empresas emp ON emp.empres_id = conv.empres_id" +
					  " WHERE conv_id = " + pConvId;

		 BD BD = new BD();
		 using (SafeDataReader dr = BD.GetDataReader(sql, null))
		 {
			if (dr.Read())
			{
			   conv.Conv_id = pConvId;
			   conv.Chapa = Convert.ToInt64(dr.GetObject("chapa"));
			   conv.Titular = dr.GetString("titular").ToUpper();
			   conv.Status = dr.GetString("liberado");
			   conv.Empresa.Empres_id = dr.GetInt32("empres_id");
			   conv.Empresa.Prog_desc = dr.GetString("prog_desc");
			   conv.DtNasc = dr.GetDateTime("dt_nascimento").ToString("dd/MM/yyyy");
			   conv.Cpf = dr.GetString("cpf");
			   conv.Rg = dr.GetString("rg");
			   conv.Endereco.Logradouro = dr.GetString("endereco").ToUpper();
			   conv.Endereco.Numero = dr.GetInt32("resnumero");
			   conv.Endereco.Bairro = dr.GetString("bairro").ToUpper();
			   conv.Endereco.Cep = dr.GetString("cep");
			   conv.Endereco.Cidade = dr.GetString("cidade").ToUpper();
			   conv.Endereco.Uf = dr.GetString("estado").ToUpper();
			   conv.Telefone1 = dr.GetString("telefone1");
			   conv.Cargo = dr.GetString("cargo").ToUpper();
			   conv.Setor = dr.GetString("setor").ToUpper();
			   conv.Obs1 = dr.GetString("obs1").ToUpper();
			   conv.Obs2 = dr.GetString("obs2").ToUpper();
			   conv.LimiteMes = Convert.ToSingle(dr.GetDecimal("limite_mes"));
			   conv.Grupo_conv_emp = dr.GetInt32("grupo_conv_emp");
			   conv.Email = dr.GetString("email");
			   conv.Usa_saldo_dif = dr.GetString("usa_saldo_dif");
			}
			dr.Close();
		 }
		 return conv;
	  }

	  public static DataTable GetConveniadoTable(string chapa, int empres_id)
	  {
		 BD BD = new BD();
		 return BD.GetDataTable(
			   "SELECT * FROM conveniados WHERE apagado <> 'S' AND empres_id = " + empres_id + " AND chapa = " + chapa, null);
	  }

	  //Alterado para SqlService
	  public static DataTable GetConveniadoTablePorConvId(string conv_id)
	  {
		BD BD = new BD();
		return BD.GetDataTable(
			  "SELECT * FROM conveniados WHERE apagado <> 'S' AND conv_id = " + conv_id, null);
	  }


	  //Alterado para SqlServer
	  public static int GetConveniadoProtocolo()
	  {
		BD BD = new BD();
		return Convert.ToInt32(BD.ExecuteScalar("SELECT NEXT VALUE FOR SLPROTOCOLO ", null));
	  }

	  //Alterado para SqlServer
	  public static DataTable GetLimitesConv(int conv_id)
	  {
		BD BD = new BD();
		return BD.GetDataTable("select top 1 coalesce(limite_1,0) limite_mes, coalesce(limite_2,0) limite_mes_2, coalesce(limite_3,0) limite_mes_3, coalesce(limite_4,0) limite_mes_4 from bandeiras_conv where conv_id = "+conv_id.ToString(), null);
	  }

	  public static Bandeiras_Conv GetBandeirasConv(int conv_id)
	  {
		Bandeiras_Conv bc = new Bandeiras_Conv();
		string sql = " SELECT coalesce(conv_id,-1) conv_id, coalesce(limite_1,0) limite_1, coalesce(limite_2,0) limite_2, " +
					 " coalesce(limite_3,0) limite_3, coalesce(limite_4,0) limite_4 FROM BANDEIRAS_CONV WHERE CONV_ID  = " + conv_id;

		BD BD = new BD();
		bc.Conv_id = -1;
		using (SafeDataReader dr = BD.GetDataReader(sql, null))
		{
		  if (dr.Read())
		  {
			bc.Conv_id = dr.GetInt32("conv_id");
			bc.Limite_1 = dr.GetFloat("limite_1");
/*            if (bc.Limite_1 != 0)
			  bc.Conv_id = conv_id;
			else
			  bc.Conv_id = -1;*/
			bc.Limite_2 = dr.GetFloat("limite_2");
			bc.Limite_3 = dr.GetFloat("limite_3");
			bc.Limite_4 = dr.GetFloat("limite_4");
		  }
		}
		return bc;
	  }
	 
	  public static DataTable GetBandeirasConvTable(int conv_id)
	  {
		BD BD = new BD();
		return BD.GetDataTable("SELECT * FROM BANDEIRAS_CONV WHERE CONV_ID  = " + conv_id, null);
	  }

	  //Alterado para SqlServer
	  public static bool IsDemitido(string conv_id, out DateTime DataDemiss)
	  {
		 BD BD = new BD();
		 object dem = BD.ExecuteScalar("SELECT data_demissao FROM conveniados WHERE conv_id = " + conv_id, null);
		 if ((dem == System.DBNull.Value) || (dem == null))
		 {
			dem = null;
			DataDemiss = DateTime.Now;
		 }
		 else
			DataDemiss = (DateTime)dem;
		 return (dem != null);
	  }

	  //Alterado para SqlServer
	  private static decimal GetSaldoDevedor(string conv_id)
	  {
		 string sql = "SELECT Coalesce(SUM(debito-credito),0) FROM contacorrente WHERE COALESCE(baixa_conveniado,'N')='N' AND conv_id =" + conv_id;

		 try
		 {
			BD BD = new BD();
			return Convert.ToDecimal(BD.GetOneRow(sql, null).ItemArray[0]);
		 }
		 catch // o sum é null
		 {
			return 0;
		 }
	  }

	  //Alterado para SqlServer
	  private static decimal GetSaldoDevedorFaturado(string conv_id)
	  {
		 string sql = "SELECT coalesce(SUM(debito-credito),0) FROM contacorrente WHERE COALESCE(baixa_conveniado,'N')='N' AND COALESCE(fatura_id,0)>0 AND conv_id =" + conv_id;

		 try
		 {
			BD BD = new BD();
			return Convert.ToDecimal(BD.GetOneRow(sql, null).ItemArray[0]);
		 }
		 catch // o sum é null
		 {
			return 0;
		 }
	  }

	  //Alterado para SqlServer
	  public static void AlteraDemitido(string conv_id, DateTime datademissao, string operador, int nProtocolo)
	  {
		 int ok = 0;
		 decimal saldodev = GetSaldoDevedor(conv_id);
		 decimal saldodevfat = GetSaldoDevedorFaturado(conv_id);
		 BD BD = new BD();
		 DataRow cadastrado = BD.GetOneRow("SELECT data_demissao, saldo_devedor, saldo_devedor_fat FROM conveniados WHERE conv_id = " + conv_id, null);

		 string sql = "UPDATE conveniados SET data_demissao ='" + datademissao.ToString("dd/MM/yyyy") +
													"', saldo_devedor ='" + saldodev.ToString("#####0.00").Replace(",", ".") +
													"',  saldo_devedor_fat ='" + saldodevfat.ToString("#####0.00").Replace(",", ".") +
													"' WHERE conv_id = " + conv_id;
		 
		 ok = BD.ExecuteNoQuery(sql, null);

		 if (ok == 1)
		 {
			int logID = Log.GeraLogID();
			Log.GravaLog(logID, "FCadConv", "Data Demissao", Convert.ToDateTime(cadastrado.ItemArray[0]).ToString("dd/MM/yyyy"), datademissao.ToString("dd/MM/yyyy"), operador, "Alteração", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, "", nProtocolo);

			if (!(Convert.ToDouble(cadastrado.ItemArray[1]).ToString("N2").Equals(saldodev.ToString("N2"))))
			{
				logID = Log.GeraLogID();
				Log.GravaLog(logID, "FCadConv", "Saldo Dev.", Convert.ToDouble(cadastrado.ItemArray[1]).ToString("N2"), saldodev.ToString("N2"), operador, "Alteração", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, "", nProtocolo);
			}
			if (!(Convert.ToDouble(cadastrado.ItemArray[2]).ToString("N2").Equals(saldodevfat.ToString("N2"))))
			{
				logID = Log.GeraLogID();
				Log.GravaLog(logID, "FCadConv", "Saldo Dev. Fat.", Convert.ToDouble(cadastrado.ItemArray[2]).ToString("N2"), saldodevfat.ToString("N2"), operador, "Alteração", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, "", nProtocolo);
			}
		 }
	  }

	  //Alterado para SqlServer
	  public static void GravaDemitido(string conv_id, DateTime datademissao, string operador, int nProtocolo)
	  {
		 int ok = 0;
		 BD BD = new BD();
		 decimal saldodev = GetSaldoDevedor(conv_id);
		 decimal saldodevfat = GetSaldoDevedorFaturado(conv_id);

		 string sql = "UPDATE CONVENIADOS SET data_demissao ='" + datademissao.ToString("dd/MM/yyyy") +
												"', saldo_devedor =" + saldodev.ToString("#####0.00").Replace(",", ".") +
												",  saldo_devedor_fat =" + saldodevfat.ToString("#####0.00").Replace(",", ".") +
												" WHERE conv_id = " + conv_id;            
		 ok = BD.ExecuteNoQuery(sql, null);

		 if (ok == 1)
		 {
			if ((DALMisc.UsaNovoFechamento()) && (DALMisc.Demissao_Move_Auts()))
			{
			  sql = "UPDATE contacorrente SET data_fecha_emp = '" + GetProxFechamentoAberto(DateTime.Now,conv_id) + "' WHERE fatura_id = 0 AND COALESCE(baixa_conveniado,'N') <> 'S' AND conv_id =" + conv_id + " and data_fecha_emp > '" + GetProxFechamentoAberto(DateTime.Now,conv_id) + "'";
			  BD.ExecuteNoQuery(sql, null);
			}
			int logID = Log.GeraLogID();
			Log.GravaLog(logID, "FCadConv", "Data Demissao", "", datademissao.ToString("dd/MM/yyyy"), operador, "Alteração", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, "", nProtocolo);
			logID = Log.GeraLogID();
			Log.GravaLog(logID, "FCadConv", "Saldo Dev.", "", saldodev.ToString("N2"), operador, "Alteração", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, "", nProtocolo);
			logID = Log.GeraLogID();
			Log.GravaLog(logID, "FCadConv", "Saldo Dev. Fat.", "", saldodevfat.ToString("N2"), operador, "Alteração", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, "", nProtocolo);
		 }
	  }

	  //Alterado para SqlServer
	  public static DateTime GetProxFechamentoAberto(DateTime data, string conv_id, int empres_id = 0)
	  {
		  BD BD = new BD();
		  if (empres_id <= 0)
		  {
			  empres_id = Convert.ToInt32(BD.ExecuteScalar("select empres_id from conveniados where conv_id = " + conv_id, null));
		  }

		  string sql = " select min(data_fecha) from dia_fecha where data_fecha > '" + data + "' and empres_id = " + empres_id + "and data_fecha not in "
			  + " (Select fechamento from fatura where apagado <> 'S' and tipo = 'E' and id = " + empres_id + ")"
			  + " and data_fecha not in (Select fechamento from fatura where apagado <> 'S' and tipo = 'C' and id = " + conv_id + ")";
		  return Convert.ToDateTime(BD.ExecuteScalar(sql, null));
	  }

	  public static void GravaDemitidoSemMoverAuts(string conv_id, DateTime datademissao, string operador)
	  {
		int ok = 0;
		BD BD = new BD();
		DataTable cadastrado = BD.GetDataTable("SELECT data_demissao, saldo_devedor, saldo_devedor_fat FROM conv_detail WHERE conv_id = " + conv_id, null);
		decimal saldodev = GetSaldoDevedor(conv_id);
		decimal saldodevfat = GetSaldoDevedorFaturado(conv_id);

		if (cadastrado.Rows.Count == 0)
		{
		  string sql = "INSERT INTO conv_detail (conv_id, data_demissao, saldo_devedor, saldo_devedor_fat) VALUES (" +
							   conv_id + ",'" + datademissao.ToString("MM/dd/yyyy") + "','" +
							   saldodev.ToString("#####0.00").Replace(",", ".") + "','" + saldodevfat.ToString("#####0.00").Replace(",", ".") + "')";

		  ok = BD.ExecuteNoQuery(sql, null);
		}
		else
		{
		  string sql = "UPDATE conv_detail SET data_demissao ='" + datademissao.ToString("MM/dd/yyyy") +
												  "', saldo_devedor ='" + saldodev.ToString("#####0.00").Replace(",", ".") +
												  "',  saldo_devedor_fat ='" + saldodevfat.ToString("#####0.00").Replace(",", ".") +
												  "' WHERE conv_id = " + conv_id;
		  ok = BD.ExecuteNoQuery(sql, null);

		}

		if (ok == 1)
		{
		  int logID = Log.GeraLogID();      
		  //Log.GravaLog(logID,"FCadConv", "Data Demissao", "", datademissao.ToString("dd/MM/yyyy"), operador, "Alteração", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, "");
		  logID = Log.GeraLogID();
		  //Log.GravaLog(logID,"FCadConv", "Saldo Dev.", "", saldodev.ToString("N2"), operador, "Alteração", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, "");
		  logID = Log.GeraLogID();
		  //Log.GravaLog(logID,"FCadConv", "Saldo Dev. Fat.", "", saldodevfat.ToString("N2"), operador, "Alteração", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, "");
		}
	  }

	  //Alterado para SqlServer
	  public static void TiraDemitido(string conv_id, string operador, int nProtocolo)
	  {
		 int ok = 0;
		 BD BD = new BD();
		 DataRow cadastrado = BD.GetOneRow("SELECT data_demissao, saldo_devedor, saldo_devedor_fat FROM conveniados WHERE conv_id = " + conv_id, null);
		 
		 ok = BD.ExecuteNoQuery("UPDATE conveniados SET data_demissao = null, saldo_devedor = '0', saldo_devedor_fat = '0' WHERE conv_id = " + conv_id, null);

		 if (ok == 1)
		 {
			int logID = Log.GeraLogID();
			Log.GravaLog(logID, "FCadConv", "Data Demissao", Convert.ToDateTime(cadastrado.ItemArray[0]).ToString("dd/MM/yyyy"), "", operador, "Alteração", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, "", nProtocolo);

			if (!(Convert.ToDouble(cadastrado.ItemArray[1]).ToString("N2").Equals("0,00")))
			{
				logID = Log.GeraLogID();
				Log.GravaLog(logID, "FCadConv", "Saldo Dev.", Convert.ToDouble(cadastrado.ItemArray[1]).ToString("N2"), "0,00", operador, "Alteração", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, "", nProtocolo);
			}

			if (!(Convert.ToDouble(cadastrado.ItemArray[2]).ToString("N2").Equals("0,00")))
			{
				logID = Log.GeraLogID();
				Log.GravaLog(logID, "FCadConv", "Saldo Dev. Fat.", Convert.ToDouble(cadastrado.ItemArray[2]).ToString("N2"), "0,00", operador, "Alteração", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, "", nProtocolo);
			}
		 }
	  }

	  //Alterado para SqlServer
	  public static int SetStatus(string conv_id, bool status, string motivo, string operador, int nProtocolo)
	  {
		 int ok = 0;

		 if (status)
		 {
			BD BD = new BD();
			ok = BD.ExecuteNoQuery("UPDATE conveniados SET liberado = 'S', operador = '" + operador + "', dtalteracao = '" +
						   DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "' WHERE conv_id = " + conv_id, null);
			if (ok == 1)
			{
				int logID = Log.GeraLogID();
				Log.GravaLog(logID, "FCadConv", "Liberado", "N", "S", operador, "Alteração", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, "", nProtocolo);
			}
		 }
		 else
		 {
			BD BD = new BD();
			ok = BD.ExecuteNoQuery("UPDATE conveniados SET liberado = 'N', operador = '" + operador + "', dtalteracao = '" +
						   DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "' WHERE conv_id = " + conv_id, null);
			if (ok == 1)
			{
				int logID = Log.GeraLogID();
				Log.GravaLog(logID, "FCadConv", "Liberado", "S", "N", operador, "Alteração", "Cadastro de Conveniados", conv_id, "Conv ID: " + conv_id, motivo, nProtocolo);
			}
		 }
		 return ok;
	  }

	  //Alterado para SqlServer
	  public static bool VerificaChapaExiste(double chapa, int empres_id)
	  {
		 BD BD = new BD();
		 if ((chapa != 0) && (empres_id != 0))
			return (BD.ExecuteScalar("SELECT chapa FROM conveniados WHERE chapa = " + chapa + " AND empres_id = " + empres_id, null) != null);
		 else
			return false;
	  }

	  public static DataTable AlteracoesRecentes(string data, int empres_id)
	  {
		 string sql = "select UPPER(LG.CAMPO) CAMPO, UPPER(LG.VALOR_ANT) VALOR_ANT, UPPER(LG.VALOR_POS) VALOR_POS, UPPER(LG.OPERADOR) OPERADOR, LG.DATA_HORA, CONV.CHAPA, UPPER(CONV.TITULAR) TITULAR";
		 sql += " from LOGS LG, CONVENIADOS CONV where LG.JANELA = 'FCadConv' ";
		 sql += " and LG.OPERACAO <> 'Inclusão'";
		 sql += " and LG.ID = CONV.CONV_ID and CONV.EMPRES_ID =" + empres_id;
		 sql += " and LG.DATA_HORA >='" + data;
		 sql += " ' order by LG.DATA_HORA desc";

		 BD BD = new BD();
		 return BD.GetDataTable(sql, null);
	  }

	  //Alterado para SqlServer
	  public static DataTable CadastrosRecentes(string data, int empres_id)
	  {
		 string sql = "select CHAPA, upper(TITULAR) TITULAR, LIBERADO, LIMITE_MES,";
		 sql += " DTCADASTRO, upper(OPERADOR) OPERADOR from CONVENIADOS where APAGADO='N'";
		 sql += " and EMPRES_ID=" + empres_id;
		 sql += " and DTCADASTRO >='" + data;
		 sql += " ' order by DTCADASTRO desc";

		 BD BD = new BD();
		 return BD.GetDataTable(sql, null);
	  }

	  //Alterado para SqlServer
	  public static int GetQtdLimites(int empres_id)
	  {
		BD BD = new BD();
		int qtdLimites = Convert.ToInt32(BD.ExecuteScalar("SELECT qtd_limites FROM bandeiras WHERE band_id = (SELECT band_id FROM empresas WHERE empres_id = " + empres_id.ToString() + ")", null).ToString());
		return qtdLimites;
	  }

	  //Alterado para SqlServer
	  public static int GetConvQtdLimites(int conv_id)
	  {
		BD BD = new BD();
		int qtdLimites = Convert.ToInt32(BD.ExecuteScalar("SELECT qtd_limites FROM bandeiras WHERE band_id = (SELECT band_id FROM empresas WHERE empres_id = (SELECT empres_id from conveniados where conv_id = " + conv_id.ToString() + "))", null).ToString());
		return qtdLimites;
	  }


	  //Alterado para SqlServer
	  public static DataTable ConveniadosAlimentacao(int empres_id)
	  {
		  string sql = "select  conv_id, upper(TITULAR) TITULAR, LIMITE_MES, ABONO_MES, SALDO_RENOVACAO";
		  sql += "  from CONVENIADOS where APAGADO='N'";
		  sql += " and EMPRES_ID=" + empres_id;
		  sql += "  order by titular";

		  BD BD = new BD();
		  return BD.GetDataTable(sql, null);
	  }

	  public static int GetUpCreditoAlim(int renovacaoID, int conv_id, float limiteMes, int dias, float saldoRenovacao, float valorAbono)
	  {
		  BD BD = new BD();

		  valorAbono = valorAbono * dias;
	   
		  return BD.ExecuteNoQuery("UPDATE alimentacao_renovacao_creditos SET renovacao_valor = " + Utils.decimalSql(saldoRenovacao) + ", abono_valor = " + Utils.decimalSql(valorAbono) + ", dias_trab = " + dias + ", data_alteracao = '" + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "'  WHERE renovacao_id = " + renovacaoID + " and conv_id = " + conv_id, null);
	 
	  }

	  public static int GetUpSaldoDevedor(int conv_id, float saldo)
	  {
		  BD BD = new BD();

		  return BD.ExecuteNoQuery("UPDATE conveniados SET saldo_devedor = " + Utils.decimalSql(saldo) + " where conv_id = " + conv_id, null);

	  }

	  public static bool GetInserirCreditos(int renovacaoID, int empres_id, float renovacaoValor, float abonoValor)
	  {
		  BD BD = new BD();
		  int incluiu;

		  BD.ExecuteNoQuery("DELETE FROM ALIMENTACAO_RENOVACAO_CREDITOS WHERE RENOVACAO_ID = " + renovacaoID, null);

		  incluiu = BD.ExecuteNoQuery("INSERT INTO ALIMENTACAO_RENOVACAO_CREDITOS SELECT " + renovacaoID + ", CONV_ID," + Utils.decimalSql(renovacaoValor) + ","
			  + Utils.decimalSql(abonoValor) + ", 0,'" + DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss") + "' FROM conveniados WHERE EMPRES_ID = " + empres_id + " AND APAGADO <> 'S' AND LIBERADO = 'S'", null);

		  if (incluiu == 0)
		  {
			  return false;
		  }
		  else
			  return true;
	   

	  }

	  //Alterado para SqlServer
	  public static DataTable PesquisaPaginadaAli(int page, int empres_id, string chapa, string cartao, string nome, string status, string grupo)
	  {
		  int band_id = GetEmpresBandId(empres_id);

		  string sql;

		  sql = " DECLARE @TamanhoPagina INT; ";
		  sql += " DECLARE @NumeroPagina INT; ";
		  sql += " SET @TamanhoPagina = 20; ";
		  sql += " SET @NumeroPagina = " + page + ";";
		  sql += " WITH Paginado AS ( ";
		  sql += " SELECT ROW_NUMBER() OVER(ORDER BY CONV.TITULAR) AS linha, ";
		  sql += "        conv.conv_id,";
		  sql += "        conv.titular, \n";
		  sql += "        coalesce(conv.limite_mes,0.00) limite_mes, \n";
		  sql += "        coalesce(alc.dias_trab,0) dias_trab, \n";
		  sql += "        coalesce(alc.abono_valor,0.00) abono_mes, \n";
		  sql += "        coalesce(alc.renovacao_valor, 0.00) saldo_renovacao, \n"; 
		  sql += "        alr.RENOVACAO_ID, \n";
		  sql += "        alr.DATA_RENOVACAO, \n";
		  sql += "        alr.TIPO_CREDITO \n";
		  sql += "   FROM conveniados conv \n";
		  sql += "   JOIN Cartoes cart ON cart.conv_id = conv.conv_id \n";
		  sql += "   left join ALIMENTACAO_RENOVACAO alr on alr.EMPRES_ID = conv.EMPRES_ID \n";
		  sql += "   left join ALIMENTACAO_RENOVACAO_CREDITOS alc on alc.RENOVACAO_ID = alr.RENOVACAO_ID and alc.CONV_ID = conv.CONV_ID \n";

		  string where = " WHERE conv.apagado <> 'S' AND cart.apagado <> 'S' AND cart.liberado <> 'I' AND conv.empres_id = " + empres_id;

		  if (grupo != "0")
			  where += " AND conv.grupo_conv_emp=" + grupo;

		  netUtil.Funcoes funcoes = new netUtil.Funcoes();

		  if (chapa != "")
			  where += " AND conv.chapa = " + chapa;
		  else
			  if (cartao != "") //procura pelo cartao
			  {
				  string codigo = "";
				  string digito = "";
				  if (cartao.Length > 3)
				  {
					  codigo = cartao.Substring(0, cartao.Length - 2);
					  digito = cartao.Substring(cartao.Length - 2, 2);

					  where += " AND(( cart.codigo = " + codigo;
					  where += " AND cart.digito = " + digito + " )";
					  where += " OR (cart.codcartimp = '" + cartao + "'))";
				  }
				  else
					  where += " AND cart.codcartimp ='" + cartao + "'";
			  }
			  else
				  if (nome != "")//procura pelo Nome
					  where += " AND (conv.titular LIKE '" + (nome.Length > 3 ? "%" : "") + nome.ToUpper() + "%')";

		  
		  where += " AND conv.liberado = '" + status + "'";
   
		  string paginado = ") SELECT TOP (@TamanhoPagina) * FROM Paginado p WHERE linha > @TamanhoPagina * (@NumeroPagina - 1)  ORDER BY titular ";

		  BD BD = new BD();
		  return BD.GetDataTable(sql + where + paginado, null);
	  }


	  //public static int PesqPagVirtualItemCount(int empres_id, string chapa, string cartao, string nome, string status, string grupo)
	  //{
	  //    string where = " WHERE conv.apagado <> 'S' AND cart.apagado <> 'S' AND conv.empres_id = " + empres_id;

	  //    if (grupo != "0")
	  //        where += " AND conv.grupo_conv_emp=" + grupo;

	  //    netUtil.Funcoes funcoes = new netUtil.Funcoes();

	  //    if (chapa != "")
	  //        where += " AND conv.chapa = " + chapa;
	  //    else
	  //        if (cartao != "") //procura pelo cartao
	  //        {
	  //            string codigo = "";
	  //            string digito = "";
	  //            if (cartao.Length > 3)
	  //            {
	  //                codigo = cartao.Substring(0, cartao.Length - 2);
	  //                digito = cartao.Substring(cartao.Length - 2, 2);

	  //                where += " AND(( cart.codigo = " + codigo;
	  //                where += " AND cart.digito = " + digito + " )";
	  //                where += " OR (cart.codcartimp = '" + cartao + "'))";
	  //            }
	  //            else
	  //                where += " AND cart.codcartimp ='" + cartao + "'";
	  //        }
	  //        else
	  //            if (nome != "")//procura pelo Nome
	  //                where += " AND (conv.titular LIKE '" + (nome.Length > 3 ? "%" : "") + nome.ToUpper() + "%')";

	  //    if (!status.Equals("T"))
	  //        where += " AND conv.liberado = '" + status + "'";

	  //    BD BD = new BD();
	  //    return BD.ExecuteScalar<Int32>(0, " SELECT DISTINCT count(*) FROM Conveniados conv JOIN Cartoes cart ON conv.conv_id = cart.conv_id and cart.titular = 'S' " + where, null);
	  //}

	  #endregion

	  #region WebEmpresas, WebUsuarios
	  public static bool ParticipaProgramaDesconto(int conv_id)
	  {
		 string sql = " SELECT COUNT(p.prog_id)";
		 sql += " FROM Programas p";
		 sql += " WHERE (p.prog_id IN (";
		 sql += "    SELECT prog_id FROM Prog_Empr WHERE empres_id = (SELECT empres_id FROM Conveniados WHERE conv_id =" + conv_id + "))";
		 sql += " OR p.prog_id IN (";
		 sql += "    SELECT prog_id FROM Prog_Conv WHERE conv_id = " + conv_id + "))";
		 sql += " AND p.apagado <> 'S'";
		 BD BD = new BD();
		 int count = (int)BD.ExecuteScalar(sql, null);

		 return count > 0;
	  }
	  #endregion

	  #region WebUsuarios
	  //public static DataTable GetDadosLogin(string tipo, string cartao, string cpf)
	  //{
	  //   netUtil.Funcoes func = new netUtil.Funcoes();

	  //   string sql = " SELECT conv.conv_id, conv.senha, cart.cartao_id FROM conveniados conv" +
	  //                " JOIN cartoes cart ON (cart.conv_id = conv.conv_id) " +
	  //                " WHERE cart.apagado <> 'S' AND conv.apagado <> 'S' ";

	  //   if (tipo.Equals("cartao"))
	  //   {
	  //      string codigo = "";
	  //      string digito = "";
	  //      if (cartao.Length > 3)
	  //      {
	  //         codigo = cartao.Substring(0, cartao.Length - 2);
	  //         digito = cartao.Substring(cartao.Length - 2, 2);

	  //         sql += " AND(( cart.codigo = " + codigo;
	  //         sql += " AND cart.digito = " + digito + " )";
	  //         sql += " OR (cart.codcartimp = '" + cartao + "'))";
	  //      }
	  //      else
	  //         sql += " AND cart.codcartimp ='" + cartao + "'";
	  //   }
	  //   else
	  //      sql += " AND cart.titular = 'S' AND (conv.cpf = '" + cpf + "' OR conv.cpf = '" + cpf.Replace(".", "").Replace("-", "") + "')";

	  //   BD BD = new BD();
	  //   return BD.GetDataTable(sql, null);
	  //}

	  //Alterado para SqlServer
	  public static int AlteraSenha(int conv_id, string senha)
	  {
		 BD BD = new BD();
		 return BD.ExecuteNoQuery("UPDATE conveniados SET senha = '" + senha + "' WHERE conv_id = " + conv_id, null);
	  }

	  public static int GetQtdLimitesPorBandId(int band_id)
	  {
		BD BD = new BD();
		int retorno = Convert.ToInt32(BD.ExecuteScalar("select qtd_limites from bandeiras where band_id = " + band_id, null));
		return retorno;
	  }

	  //Alterado para SqlServer
	  public static int GetQtdLimitesPorEmpresId(int empres_id)
	  {
		BD BD = new BD();
		int retorno = Convert.ToInt32(BD.ExecuteScalar("select qtd_limites from bandeiras where band_id = (select top 1 band_id from empresas where empres_id = " + empres_id + ")", null));
		return retorno;
	  }

	  public static DataRow GetConv(string cartao_id)
	  {
		 string sql = " SELECT conv.titular, conv.cpf, cart.nome, emp.fantasia as empresa, \n";
		 sql += "--PEGANDO O LIMITE-- \n";
		 sql += "   coalesce(case \n";
		 sql += "     when emp.band_id <> 999 then \n";
		 sql += "      (case \n";
		 sql += "     when conv.conv_id in \n";
		 sql += "          (select conv.conv_id \n";
		 sql += "             from bandeiras_conv bConv \n";
		 sql += "            where bConv.conv_id = conv.conv_id) then \n";
		 sql += "      bc.limite_1 \n";
		 sql += "     else \n";
		 sql += "      b.limite_1 \n";
		 sql += "   end) else conv.limite_mes end,0) as limite_mes, \n";
		 sql += "--PEGANDO O LIMITE 2 -- \n";
		 sql += "   coalesce(case \n";
		 sql += "     when emp.band_id <> 999 then \n";
		 sql += "      (case \n";
		 sql += "     when conv.conv_id in \n";
		 sql += "          (select conv.conv_id \n";
		 sql += "             from bandeiras_conv bConv \n";
		 sql += "            where bConv.conv_id = conv.conv_id) then \n";
		 sql += "      bc.limite_2 \n";
		 sql += "     else \n";
		 sql += "      b.limite_2 \n";
		 sql += "   end) else conv.limite_mes end,0) as limite_mes_2, \n";
		 sql += "--PEGANDO O LIMITE 3 -- \n";
		 sql += "   coalesce(case \n";
		 sql += "     when emp.band_id <> 999 then \n";
		 sql += "      (case \n";
		 sql += "     when conv.conv_id in \n";
		 sql += "          (select conv.conv_id \n";
		 sql += "             from bandeiras_conv bConv \n";
		 sql += "            where bConv.conv_id = conv.conv_id) then \n";
		 sql += "      bc.limite_3 \n";
		 sql += "     else \n";
		 sql += "      b.limite_3 \n";
		 sql += "   end) else conv.limite_mes end,0) as limite_mes_3, \n";
		 sql += "--PEGANDO O LIMITE 4 -- \n";
		 sql += "   coalesce(case \n";
		 sql += "     when emp.band_id <> 999 then \n";
		 sql += "      (case \n";
		 sql += "     when conv.conv_id in \n";
		 sql += "          (select conv.conv_id \n";
		 sql += "             from bandeiras_conv bConv \n";
		 sql += "            where bConv.conv_id = conv.conv_id) then \n";
		 sql += "      bc.limite_4 \n";
		 sql += "     else \n";
		 sql += "      b.limite_4 \n";
		 sql += "   end) else conv.limite_mes end,0) as limite_mes_4, \n";
		 //sql += " (case when (select count(qtdLimite) from banderias band where  band.band_id = b.band_id and band.empres_id = emp.empres_id) > 0 then";
		 //sql += " select coalesce(qtdLimite,1) from banderias band where  band.band_id = b.band_id and band.empres_id = emp.empres_id else 1 end) as qtdLimite,";
		 sql += " conv.email, conv.liberado convlib, conv.empres_id, cart.liberado cartlib, \n";
		 sql += " cart.codigo, cart.digito, cart.cartao_id, conv.fidelidade AS convfidelidade, emp.fidelidade AS empfidelidade, emp.prog_desc, emp.band_id \n";
		 sql += " FROM Conveniados conv \n";
		 sql += " JOIN Cartoes cart ON cart.conv_id = conv.conv_id \n";
		 sql += " JOIN Empresas emp ON conv.empres_id = emp.empres_id \n";
		 sql += " JOIN bandeiras b ON b.band_id = emp.band_id \n";
		 sql += " LEFT JOIN bandeiras_conv bc ON conv.conv_id = bc.conv_id \n";
		 sql += " WHERE cart.apagado <> 'S' \n";
		 sql += " AND conv.apagado <> 'S' \n";
		 sql += " AND emp.apagado <> 'S' \n";
		 sql += " AND cart.cartao_id =" + cartao_id;
		 BD BD = new BD();
		 return BD.GetOneRow(sql, null);
	  }

	  public static DataTable GetSegmentos(int conv_id, int limite = -1)
	  {
		 SqlParamsList ps = new SqlParamsList();
		 ps.Add(new Fields("conv_id", conv_id));
		 string sql = "SELECT descricao, limite_seg"+ (limite == -1?"":(limite > 1 && limite < 5?"_"+limite:"")) +", restante FROM seg_conv(@conv_id) ORDER BY descricao";
		 BD BD = new BD();
		 return BD.GetDataTable(sql, ps);
	  }

	  //Alterado para SqlServer
	  public static DataTable GetFechamentos(int conv_id)
	  {
		 SqlParamsList ps = new SqlParamsList();
		 ps.Add(new Fields("conv_id", conv_id));

		 string sql = "SELECT convert(varchar,data_fecha,103) as data_format, data_fecha FROM dia_fecha" +
					 " WHERE empres_id = (SELECT empres_id FROM conveniados WHERE conv_id = @conv_id)" +
					 " AND data_fecha BETWEEN '" + DateTime.Today.AddMonths(-6).ToString("dd/MM/yyyy") + "' AND '" +
					 DateTime.Today.AddMonths(6).ToString("dd/MM/yyyy") + "' ORDER BY data_fecha";

		 BD BD = new BD();
		 return BD.GetDataTable(sql, ps);
	  }
	  #endregion

	  //Alterado para SqlServer
	  public static bool ExisteCPFCadastrado(string cpf)
	  {
		 cpf = cpf.Replace(".","").Replace("-","");

		 SqlParamsList ps = new SqlParamsList();
		 ps.Add(new Fields("cpf", cpf));         

		 string sql = " SELECT conv_id";
		 sql += " FROM Conveniados";
		 sql += " WHERE apagado <> 'S'";
		 sql += " AND (REPLACE(REPLACE(cpf,'.',''),'-','') = @cpf)";         

		 BD BD = new BD();
		 DataTable dt = BD.GetDataTable(sql, ps);

		 return dt.Rows.Count > 0;
	  }

	  //Alterado para SqlServer
	  public static string getSenhaConv(string cartao)
	  {
		SqlParamsList ps = new SqlParamsList();
		ps.Add(new Fields("cartao", cartao));

		string sql = " SELECT cart.senha ";
		sql += " FROM CARTOES cart where cart.codcartimp = @cartao and cart.apagado <> 'S'";

		BD BD = new BD();

		return BD.ExecuteScalar(sql, ps).ToString();
	  }

	  //Alterado para SqlServer 
	  public static bool getCartao(string cartao)
	  {
		  SqlParamsList ps = new SqlParamsList();
		  ps.Add(new Fields("cartao", cartao));

		  string sql = " SELECT cart.codcartimp ";
		  sql += " FROM Conveniados c";
		  sql += " JOIN Cartoes cart on (cart.conv_id = c.conv_id) and (cart.codcartimp = @cartao) and (cart.apagado <> 'S')";
		  sql += " WHERE c.apagado <> 'S'";

		  BD BD = new BD();
		  SafeDataReader dr = BD.GetDataReader(sql, ps);
		  bool retorno = false;
		  try
		  {
			if (dr.Read())
			{
				retorno = dr.GetString(0) != null;
			}
		  }
		  finally
		  {
			  dr.Close();
		  }
		  return retorno;
	  }

	  #region Cantinex

	  public static DataTable GetDadosLoginCantinex(string tipo, string email, string cpf)
	  {
		  netUtil.Funcoes func = new netUtil.Funcoes();

		  string sql = " SELECT conv.conv_id, conv.senha_cantinex, cart.cartao_id FROM conveniados conv" +
					   " JOIN cartoes cart ON (cart.conv_id = conv.conv_id) " +
					   " WHERE cart.apagado <> 'S' AND conv.apagado <> 'S' ";

		  if (tipo.Equals("cartao"))
		  {
			  sql += "AND conv.EMAIL = '" + email.Trim() + "'";
		  }
		  else
			  sql += " AND cart.titular = 'S' AND (conv.cpf = '" + cpf + "' OR conv.cpf = '" + cpf.Replace(".", "").Replace("-", "") + "')";

		  BD BD = new BD();
		  return BD.GetDataTable(sql, null);
	  }

	  public static bool ConferirTaxaServicoCantinex(int convId)
	  {
		  string sql = "SELECT TAXA_SERVICO_CANTINA FROM CONVENIADOS WHERE CONV_ID = " + convId + " AND TAXA_SERVICO_CANTINA = 'A'";
		  bool taxa = true;
		  BD BD = new BD();
		  object b = BD.ExecuteScalar(sql, null);
		  if (b == null)
		  {
			  taxa = false;
		  }
		  return taxa;
	  }

	  public static int TaxaServicoCantinexPaga(int convId)
	  {
		  BD BD = new BD();
		  return BD.ExecuteNoQuery("UPDATE CONVENIADOS SET TAXA_SERVICO_CANTINA = 'F' WHERE CONV_ID = '" + convId + "'", null);
	  }

	  //Alterado para SqlServer
	  public static int AlteraSenhaCantinex(int conv_id, string senha)
	  {
		  BD BD = new BD();
		  return BD.ExecuteNoQuery("UPDATE conveniados SET senha_cantinex = '" + senha + "' WHERE conv_id = " + conv_id, null);
	  }

	  public static DataTable CarregarExtratosCantinex(int convId, string periodoInicial, string periodoFinal, string codCartao)
	  {
		  SqlParamsList ps = new SqlParamsList();
		  ps.Add(new Fields("conv_id", convId));
		  string sql = "SELECT CART.NOME, CART.CODCARTIMP, CONVERT(VARCHAR(12),CRED.DATA_HORA,103) AS DATA, CRECAR.VALOR FROM CANTINA_CREDITOS_CARTOES CRECAR"
					   + " INNER JOIN CANTINA_CREDITOS CRED ON(CRED.CREDITO_ID = CRECAR.CREDITO_ID)"
					   + " INNER JOIN CARTOES CART ON(CART.CARTAO_ID = CRECAR.CARTAO_ID)"
					   + " WHERE CART.CONV_ID = " + convId + " AND CRED.DATA_HORA BETWEEN"
					   + " CONCAT(CONVERT(VARCHAR, '" + periodoInicial + "', 103), ' 00:00:00')"
					   + " AND CONCAT(CONVERT(VARCHAR, '" + periodoFinal + "', 103), ' 23:59:59') ";

		  if (codCartao != "0")
		  {
			  sql += " AND CART.CODCARTIMP = '" + codCartao + "'";
		  }

		  BD BD = new BD();
		  DataTable dt = BD.GetDataTable(sql, ps);
		  return dt;
	  }

	  public static DataTable CarregarExtratosGastosCantinex(int convId, string periodoInicial, string periodoFinal, string codCartao)
	  {
		  SqlParamsList ps = new SqlParamsList();
		  ps.Add(new Fields("conv_id", convId));
		  string sql = "SELECT CART.NOME, CART.CODCARTIMP, CONVERT(VARCHAR(12),CC.DATAVENDA,103) AS DATA, CC.DEBITO AS VALOR"
					   + " FROM CONTACORRENTE CC"
					   + " INNER JOIN CARTOES CART ON(CART.CARTAO_ID = CC.CARTAO_ID)"
					   + " WHERE CART.CONV_ID = " + convId + " AND CC.DATAVENDA BETWEEN"
					   + " CONCAT(CONVERT(VARCHAR, '" + periodoInicial + "', 103), ' 00:00:00')"
					   + " AND CONCAT(CONVERT(VARCHAR, '" + periodoFinal + "', 103), ' 23:59:59') "
					   + " AND CC.CANCELADA = 'N' AND CC.DEBITO > 0 ";

		  if (codCartao != "0")
		  {
			  sql += " AND CART.CODCARTIMP = '" + codCartao + "'";
		  }

		  BD BD = new BD();
		  DataTable dt = BD.GetDataTable(sql, ps);
		  return dt;
	  }

	  public static int UpdatePasswordByCard(int cartaoId, string senha)
	  {
	      int i = 0;
		  BD BD = new BD();
		  i = Convert.ToInt32(
			  BD.ExecuteNoQuery(
				  "UPDATE CARTOES SET SENHA = '" + senha + "' WHERE CARTAO_ID = " + cartaoId, null));

	      return i;
	  }

	  public static DataTable GetDadosLogin(string tipo, string cartao, string cpf)
	  {
		  netUtil.Funcoes func = new netUtil.Funcoes();

		  string sql = " SELECT conv.conv_id, cart.senha, cart.cartao_id FROM conveniados conv" +
					   " JOIN cartoes cart ON (cart.conv_id = conv.conv_id) " +
					   " WHERE cart.apagado <> 'S' AND conv.apagado <> 'S' ";

		  if (tipo.Equals("cartao"))
		  {
			  string codigo = "";
			  string digito = "";
			  if (cartao.Length > 3)
			  {
				  codigo = cartao.Substring(0, cartao.Length - 2);
				  digito = cartao.Substring(cartao.Length - 2, 2);

				  sql += " AND(( cart.codigo = " + codigo;
				  sql += " AND cart.digito = " + digito + " )";
				  sql += " OR (cart.codcartimp = '" + cartao + "'))";
			  }
			  else
				  sql += " AND cart.codcartimp ='" + cartao + "'";
		  }
		  else
			  sql += " AND cart.titular = 'S' AND (conv.cpf = '" + cpf + "' OR conv.cpf = '" + cpf.Replace(".", "").Replace("-", "") + "')";

		  BD BD = new BD();
		  return BD.GetDataTable(sql, null);
	  }
	  #endregion
   }
}
