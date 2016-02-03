using System;
using System.Collections.Generic;
using System.Text;
using SQLHelperv2;
using Negocio;
using System.Data;
using Misc;

namespace DAL
{
   public class DALEmpresa
   {         
      #region WebEstabelecimentos

      public static bool HaEmpresasSemVendaNome()
      {
         BD BD = new BD();
         object o = BD.ExecuteScalar("SELECT count(*) FROM Empresas WHERE apagado <> 'S' AND venda_nome <> 'S' ", null);
         if (o == System.DBNull.Value || o == null || int.Parse(o.ToString()) == 0)
            return false;
         return true;
      }

      public static string GetEmpresIdsProibVendaNome()
      {
         string s = string.Empty;

         BD BD = new BD();
         SafeDataReader dr = BD.GetDataReader("SELECT empres_id FROM Empresas WHERE apagado <> 'S' AND venda_nome <> 'S' ", null);
         try
         {
             while (dr.Read())
             {
                 s += "," + dr.GetString(0);
             }
         }
         finally
         {
             dr.Close();
         }
         if (s.Length > 0)
            s = s.Remove(0, 1);
         return s;
      }

      public static string GetNome(int pEmpresaId)
      {
         string sql = "SELECT nome FROM Empresas WHERE apagado <> 'S' AND empres_id = @empId";
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("@empId", pEmpresaId));
         BD BD = new BD();
         return BD.ExecuteScalar(sql, ps).ToString();
      }

      public static int CountCartoes(StringBuilder where, int cred_id)
      {
         string S = " SELECT count(*) FROM Cartoes cart JOIN Conveniados conv ON conv.conv_id = cart.conv_id  " +
                    " AND conv.empres_id NOT IN " +
                    " (SELECT empres_id FROM cred_emp_lib WHERE cred_emp_lib.cred_id = " + cred_id + " AND cred_emp_lib.liberado = 'N') " +
                    where.ToString();
         BD BD = new BD();
         object c = BD.ExecuteScalar(S, null);
         if (c != null)
            return (int)c;
         else
            return 0;
      }

      public static DataTable EmpProibidas()
      {
         BD BD = new BD();
         return BD.GetDataTable("SELECT empres_id, nome, fantasia FROM Empresas WHERE apagado <> 'S' AND venda_nome <> 'S' ", null);
      }

      public static DataTable EmpFechaPagto()
      {
          string sql = "SELECT EMPRES_ID, NOME, FANTASIA, CASE FECHAMENTO1 WHEN 31 THEN 29  WHEN 30 THEN 29 WHEN 1 THEN 31 ELSE FECHAMENTO1 - 1 END AS CORTE,"
            + " DIA_REPASSE, OBS1 FROM EMPRESAS WHERE LIBERADA = 'S' ORDER BY NOME";
          BD BD = new BD();
          return BD.GetDataTable(sql, null);
      }

      //Alterado para SqlServer
      public static List<Empresas> GetEmpresasIncCartPbm()
      {
         string sql = "SELECT emp.empres_id, emp.nome";
         sql += " FROM Empresas emp";
         sql += " WHERE emp.inc_cart_pbm <> 'N'";
         sql += " AND emp.apagado <> 'S'";

         BD BD = new BD();
         DataTable table = BD.GetDataTable(sql, null);

         List<Empresas> lista = new List<Empresas>();
         foreach (DataRow row in table.Rows)
         {
            Empresas emp = new Empresas();
            emp.Empres_id = Convert.ToInt32(row["empres_id"]);
            emp.Nome = row["nome"].ToString();
            lista.Add(emp);
         }
         return lista;
      }

      //Alterado para SqlServer
      public static DataSet FormasPgLiberadas(int emp_id)
      {
          string sql = "SELECT A.FORMA_ID, B.DESCRICAO FROM FORMAS_EMP_LIB AS A, FORMASPAGTO AS B WHERE "
          + "A.EMP_ID = " + emp_id + " AND A.FORMA_ID = B.FORMA_ID AND B.LIBERADO = 'S' ORDER BY A.FORMA_ID";

          BD BD = new BD();
          return BD.GetDataSet(sql, null);
      }

      #endregion

      #region Relatorios WebFornecedores
    
      //Alterado para SqlServer
      public static DateTime getDataFechamento(int empresId, int mes, int ano){
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@empres_id", empresId));
        ps.Add(new Fields("@mes",mes));
        ps.Add(new Fields("@ano", ano));
        BD BD = new BD();
        return BD.ExecuteScalar(System.DateTime.Parse("01/01/1000"), "select data_fecha from dia_fecha where empres_id = @empres_id and month(data_fecha) = @mes and year(data_fecha) = @ano", ps);
      }

      public static String getCGC(int empres_id)
      {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@empres_id",empres_id));
        BD BD = new BD();
        return BD.ExecuteScalar("","select cgc from empresas where empres_id = @empres_id",ps).ToString();
      }

      //Alterado para SqlServer
      public static String getEmail(int empres_id)
      {
          SqlParamsList ps = new SqlParamsList();
          ps.Add(new Fields("@empres_id", empres_id));
          BD BD = new BD();
          return BD.ExecuteScalar("", "select email from empresas where empres_id = @empres_id", ps).ToString();
      }

      public static int getAtualizarEmail(int empres_id, string email)
      {
          SqlParamsList ps = new SqlParamsList();
          ps.Add(new Fields("@email", email));
          ps.Add(new Fields("@empres_id", empres_id));
          
          BD BD = new BD();
          return BD.ExecuteNoQuery("update empresas set email = @email where empres_id = @empres_id", ps);
      }


      //Alterado para SqlServer
      // Retorna um Datatable para ser usado como source do relatorio de Empresas</returns
      public static int getQtdDigSenha(string cartao)
      {
        string sql = string.Empty;
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("@cartao", cartao));
        BD BD = new BD();        
        return Convert.ToInt32(BD.ExecuteScalar(4,"select qtd_dig_senha from empresas where apagado <> 'S' and empres_id = (select empres_id from conveniados where apagado <> 'S' and conv_id = (select conv_id from cartoes where apagado <> 'S' and codcartimp = @cartao)) ",ps).ToString());
        
      }
      public DataTable RelatorioPorEmpresa(Extratos pExtrato, int pCredenciadoId)
      {
         string sql = string.Empty;
         string sqlfromwhere = string.Empty;
         sql = " SELECT DISTINCT conv.chapa, conv.titular, emp.empres_id, emp.nome,"; 
         sql += " cc.debito, cc.credito, cc.data, cc.nf, \n";
         sql += " (concat(CAST(cc.autorizacao_id AS varchar(10)), \n";
         sql += " (CASE WHEN digito < 10 THEN '0'+ CAST(digito AS char(1)) ELSE \n";
         sql += " CAST(digito AS char(2)) end))) AS autorizacao, \n";
         sql += " cast(debito - credito as decimal(15,2)) saldo \n";         
         sqlfromwhere = " FROM Contacorrente cc \n";
         sqlfromwhere += " JOIN Conveniados conv ON conv.conv_id  = cc.conv_id \n";          
         sqlfromwhere += " JOIN Empresas emp ON conv.empres_id = emp.empres_id \n";
         sqlfromwhere += " WHERE cc.cred_id = @credId \n";
         sqlfromwhere += " AND (cc.autorizacao_id_canc is null and cc.cancelada = 'N') \n";
         if (pExtrato.Tipo.Equals("A"))
           sqlfromwhere += " AND cc.data BETWEEN @datai AND @dataf \n";
         else
           sqlfromwhere += " AND cc.data_fecha_emp BETWEEN @datai AND @dataf \n";


         sqlfromwhere = ParamsSQL(pExtrato, sqlfromwhere);
         if (!pExtrato.TodasEmpresas)
         {
            string emps = String.Empty;
            foreach (string e in pExtrato.EmpresasMarcadas.Keys)
               emps += "," + e;
            emps = emps.Substring(1);//ignorar a primeira virgula.

            sqlfromwhere += " AND (conv.empres_id IN (" + emps + ") ) ";
         }
         sql += sqlfromwhere + " ORDER BY ";
         if (pExtrato.CampoOrdemEmp != null)
            sql += pExtrato.CampoOrdemEmp + ",";
         sql += pExtrato.CampoOrdem;

         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("@credId", pCredenciadoId));
         ps.Add(new Fields("@datai", pExtrato.DataIni.ToString("dd/MM/yyyy")));
         ps.Add(new Fields("@dataf", pExtrato.DataFim.ToString("dd/MM/yyyy")));

         BD BD = new BD();
         return BD.GetDataTable(sql, ps);
      }

      // Verifica do Fornecedor(Farmacias) as empresas com movimento no periodo indicado 
      public DataTable CriarEmpresasComMovimento(Extratos pExtrato, int pCredenciadoId)
      {
         string sql = " SELECT DISTINCT emp.empres_id, emp.nome";
         sql += " FROM Contacorrente cc";
         sql += " JOIN Conveniados conv ON conv.conv_id  = cc.conv_id";
         sql += " JOIN Empresas emp ON conv.empres_id = emp.empres_id";
         sql += " WHERE (cc.cred_id = @credId)";
         if (pExtrato.Tipo.Equals("A"))
            sql += " AND cc.data BETWEEN @datai AND @dataf";
         else
            sql += " AND cc.data_fecha_emp BETWEEN @datai AND @dataf";

         sql = ParamsSQL(pExtrato, sql) + " ORDER BY emp.nome";

         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("@credId", pCredenciadoId));
         ps.Add(new Fields("@datai", pExtrato.DataIni.ToString("dd/MM/yyyy")));
         ps.Add(new Fields("@dataf", pExtrato.DataFim.ToString("dd/MM/yyyy")));

         BD BD = new BD();
         return BD.GetDataTable(sql, ps);
      }

      // Retorna parte de uma string SQL segundo os parametros passados pelo Extrato</returns>
      private static string ParamsSQL(Extratos ext, string sql)
      {
         switch (ext.TipoReceita)
         {
            case "R": sql += " AND (COALESCE(cc.receita,'N') = 'S') "; break;
            case "S": sql += " AND (COALESCE(cc.receita,'N') = 'N') "; break;
         }
         switch (ext.TipoConfirma)
         {
            case "S": sql += " AND (COALESCE(cc.entreg_nf,'N') = 'S') "; break;
            case "N": sql += " AND (COALESCE(cc.entreg_nf,'N') = 'N') "; break;
         }
         switch (ext.TipoRecebidos)
         {
            case "S": sql += " AND (COALESCE(cc.baixa_credenciado,'N') = 'S') "; break;
            case "N": sql += " AND (COALESCE(cc.baixa_credenciado,'N') = 'N') "; break;
         }
         return sql;
      }

      // Pega as empresas do Credenciado (Farmacia) dentro dos pagamentos indicados
      // Carrega um novo DataTable para preencher as Empresas do Relatorio</returns>
      public DataTable RecebidoEmpresa(string nPagamento, int cred_id)
      {
         string sql  = " SELECT pag.id, pag.pagamento_cred_id, pag.valor, ";
                sql += " COALESCE((pag.per_comissao + pag.per_comissao_r),pag2.per_comissao, 0) AS per_comiss,";
                sql += " (COALESCE((pag.per_comissao + pag.per_comissao_r),pag2.per_comissao, 0)*pag.valor/100) AS valor_comissao, ";
                sql += " CASE WHEN (COALESCE(pag.fatura_id,0) > 0) AND (fat.tipo = 'C')";
                sql += " THEN (SELECT titular FROM Conveniados WHERE conv_id = pag.id) ELSE emp.nome END AS nome,";                
                sql += " COALESCE(pag.fatura_id,0) AS fatura_id,";
                sql += " pag2.paga_cred_por_id, pag.id as empres_id";
                sql += " FROM pagamento_cred_det pag ";
                sql += " LEFT JOIN Pagamento_cred pag2 ON pag.PAGAMENTO_CRED_ID = pag2.PAGAMENTO_CRED_ID";
                sql += " LEFT JOIN Fatura fat ON fat.fatura_id = pag.fatura_id";
                sql += " LEFT JOIN Empresas emp ON emp.empres_id = pag.id";
                sql += " WHERE pag.pagamento_cred_id IN (" + nPagamento + ")";
                sql += " AND pag2.cred_id =" + cred_id;

                BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }

      // Pega as empresas do Credenciado (Farmacia)
      // Carrega um novo DataTable para preencher as Empresas do Relatorio de Valores a Receber
      public DataTable aRecebEmpresa(string empresas, Extratos ext, int cred_id, string pExibirPagto)
      {
         StringBuilder sql = new StringBuilder();
         sql.Append(" Select coalesce(emp.empres_id,conv.conv_id) as id, fat.fechamento, fat.data_vencimento vencimento, ");
         sql.Append(" fat.fatura_id, sum(cc.debito-cc.credito) as valor, ");
         sql.Append(" coalesce(cred_emp_lib.comissao,cred.comissao) per_comiss, ");
         sql.Append(" sum(cc.debito-cc.credito)/100*coalesce(cred_emp_lib.comissao,cred.comissao) val_comiss, ");
         sql.Append(" case when coalesce(fat.tipo,'E')  = 'E' then emp.nome else conv.titular end as nome, ");
         sql.Append(" coalesce(emp.empres_id,conv.conv_id) empres_id, 0 pagamento_cred_id ");
         sql.Append(" from contacorrente cc ");
         sql.Append(" join fatura fat on fat.fatura_id = cc.fatura_id ");
         sql.Append(" join credenciados cred on cred.cred_id = cc.cred_id ");
         if (pExibirPagto == "S")
         {
            sql.Append(" join rel_fat_pag_cred rel on rel.fatura_id = fat.fatura_id ");
            sql.Append(" and rel.cred_id = cc.cred_id and (rel.exibir = 'S')");
         }
         sql.Append(" left join empresas emp on coalesce(fat.tipo,'E') = 'E' and emp.empres_id = fat.id ");
         sql.Append(" left join conveniados  conv on  fat.tipo = 'C' and conv.conv_id = fat.id ");
         sql.Append(" left join cred_emp_lib on cred_emp_lib.cred_id = cc.cred_id ");
         sql.Append(" and cred_emp_lib.empres_id = coalesce(emp.empres_id,conv.empres_id) ");
         sql.Append(" where coalesce(cc.pagamento_cred_id,0) = 0  and coalesce(fat.apagado,'N') <> 'S' ");
         if (ext.TipoRecebidos.Equals("S"))
           sql.Append(" and ((coalesce(fat.baixada,'N') = 'S') or (coalesce(fat.pre_baixa,'N') = 'S'))");
         else if (ext.TipoRecebidos.Equals("N"))
           sql.Append(" and ((coalesce(fat.baixada,'N') = 'N') or (coalesce(fat.pre_baixa,'N') = 'N'))");
         sql.Append(" and (cc.CRED_ID = " + cred_id + ") ");

         if (ext.EmpresasMarcadas != null && ext.TodasEmpresas == false)
            sql.Append(" and coalesce(emp.empres_id,conv.conv_id) in (" + empresas + ")");

         sql.Append(" group by emp.empres_id, conv.conv_id, cc.cred_id, fat.fechamento, fat.data_vencimento, fat.fatura_id,");
         sql.Append(" fat.tipo, conv.titular, emp.nome, cred_emp_lib.comissao, cred.comissao order by cc.cred_id ");
         BD BD = new BD();
         return BD.GetDataTable(sql.ToString(), null);
      }

      public bool VerificaAReceber(int pCredId)
      {
         string SQLVerificacao = " select FIRST 1 fat.fatura_id " +
                                 " from contacorrente cc join fatura fat on fat.fatura_id = cc.fatura_id" +
                                 " where coalesce(cc.pagamento_cred_id,0) = 0  and coalesce(fat.apagado,'N') = 'N'" +
                                 " and ((coalesce(fat.baixada,'N') = 'S') or (coalesce(fat.pre_baixa,'N') = 'S'))" +
                                 " and (cc.CRED_ID = " + pCredId + ") ";

         BD BD = new BD();
         int id = BD.ExecuteScalar<int>(0, SQLVerificacao, null);

         return (id > 0);
      }

      #endregion

      #region WebEmpresas, WebFornecedores

      //Alterado para SqlServer
      public static int GetTipoCredito(int empres_id)
      {
          string sql = "select top 1 tipo_credito from empresas where empres_id = @empres_id";
          BD BD = new BD();
          SqlParamsList ps = new SqlParamsList();
          ps.Add(new Fields("empres_id", empres_id));
          return BD.ExecuteScalar(-1, sql, ps);
      }


      //Alterado para Sqlserver
      public static int GetModCartao(int empres_id)
      {
          string sql = "select top 1 mod_cart_id from empresas where empres_id = @empres_id";
          BD BD = new BD();
          SqlParamsList ps = new SqlParamsList();
          ps.Add(new Fields("empres_id", empres_id));
          return BD.ExecuteScalar(-1, sql, ps);
      }

      //Alterado para SqlServer
      public static DataTable GetDadosEmpresa(int empres_id)
      {
         string sql = "select EMPRES_ID, NOME, CGC, INSCRICAOEST, TELEFONE1, FAX, REPRESENTANTE,";
         sql += "ENDERECO, NUMERO, BAIRRO, CIDADE, CEP, ESTADO, EMAIL, FECHAMENTO1, VENCIMENTO1";
         sql += " from EMPRESAS where EMPRES_ID=" + empres_id;

         BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }

      //Alterado para SqlServer
      public static string GetUsaLancCredito(int empres_id)
      {
         string sql = "SELECT realiza_lanc_credito FROM Empresas WHERE apagado <> 'S' AND empres_id = @empId";
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("@empId", empres_id));
         BD BD = new BD();
         return BD.ExecuteScalar(sql, ps).ToString();
      
      }

      public static int GetAtuDiasTrab(int empres_id, string calcula)
      {
          BD BD = new BD();
          return BD.ExecuteNoQuery("UPDATE alimentacao_renovacao SET calc_dias_trab = '" + calcula + "' WHERE empres_id = " + empres_id, null);
      }


      #endregion
                          
      #region WebEmpresas 
      
      public static void incluirEmpresasTeste()
      {
        BD BD = new BD();
        try
        {
          BD.ExecuteScalar("execute procedure INCLUIR_EMPRESAS_TESTE",null);
        }
        catch (Exception e)
        {
          throw new Exception("Erro ao incluir empresas de teste\n O erro ocorrido foi: " + e.Message);
        }
      }

      public static SqlParamsList setarParametrosEmpresa(Empresas emp, string valorNull)
      {
        SqlParamsList parametros = new SqlParamsList();
        parametros.Add(new Fields("empres_id", (emp.Empres_id.ToString() == valorNull ? "NULL" : emp.Empres_id.ToString() )));
        parametros.Add(new Fields("formaLimiteId", (emp.formaLimiteId.ToString() == valorNull ? "NULL" : emp.formaLimiteId.ToString() )));
        parametros.Add(new Fields("tipoCartao", (emp.tipoCartao.ToString() == valorNull ? "NULL" : emp.tipoCartao.ToString() )));
        parametros.Add(new Fields("credenciado", (emp.credenciado.ToString() == valorNull ? "NULL" : emp.credenciado.ToString() )));
        parametros.Add(new Fields("contrato", (emp.contrato.ToString() == valorNull ? "NULL" : emp.contrato.ToString() )));
        parametros.Add(new Fields("diaFechamento1", (emp.diaFechamento1.ToString() == valorNull ? "NULL" : emp.diaFechamento1.ToString() )));
        parametros.Add(new Fields("diaFechamento2", (emp.diaFechamento2.ToString() == valorNull ? "NULL" : emp.diaFechamento2.ToString() )));
        parametros.Add(new Fields("diaVencimento1", (emp.diaVencimento1.ToString() == valorNull ? "NULL" : emp.diaVencimento1.ToString() )));
        parametros.Add(new Fields("diaVencimento2", (emp.diaVencimento2.ToString() == valorNull ? "NULL" : emp.diaVencimento2.ToString() )));
        parametros.Add(new Fields("incCartPbm", (emp.incCartPbm.ToString() == valorNull ? "NULL" : emp.incCartPbm.ToString() )));
        parametros.Add(new Fields("prog_desc", (emp.Prog_desc.ToString() == valorNull ? "NULL" : emp.Prog_desc.ToString() )));
        parametros.Add(new Fields("nome", (emp.Nome.ToString() == valorNull ? "NULL" : emp.Nome.ToString() )));
        parametros.Add(new Fields("liberada", (emp.Liberada.ToString() == valorNull ? "NULL" : emp.Liberada.ToString() )));
        parametros.Add(new Fields("fantasia", (emp.Fantasia.ToString() == valorNull ? "NULL" : emp.Fantasia.ToString() )));
        parametros.Add(new Fields("nomeCartao", (emp.nomeCartao.ToString() == valorNull ? "NULL" : emp.nomeCartao.ToString() )));
        parametros.Add(new Fields("comissaoCred", (emp.comissaoCred.ToString() == valorNull ? "NULL" : emp.comissaoCred.ToString() )));
        parametros.Add(new Fields("fatorRisco", (emp.fatorRisco.ToString() == valorNull ? "NULL" : emp.fatorRisco.ToString() )));
        parametros.Add(new Fields("senha", (emp.senha.ToString() == valorNull ? "NULL" : emp.senha.ToString() )));
        parametros.Add(new Fields("cgc", (emp.cgc.ToString() == valorNull ? "NULL" : emp.cgc.ToString() )));
        parametros.Add(new Fields("inscricaoEst", (emp.inscricaoEst.ToString() == valorNull ? "NULL" : emp.inscricaoEst.ToString() )));
        parametros.Add(new Fields("telefone1", (emp.telefone1.ToString() == valorNull ? "NULL" : emp.telefone1.ToString() )));
        parametros.Add(new Fields("telefone2", (emp.telefone2.ToString() == valorNull ? "NULL" : emp.telefone2.ToString() )));
        parametros.Add(new Fields("fax", (emp.fax.ToString() == valorNull ? "NULL" : emp.fax.ToString() )));
        parametros.Add(new Fields("endereco", (emp.endereco.ToString() == valorNull ? "NULL" : emp.endereco.ToString() )));
        parametros.Add(new Fields("numero", (emp.numero.ToString() == valorNull ? "NULL" : emp.numero.ToString() )));
        parametros.Add(new Fields("bairro", (emp.bairro.ToString() == valorNull ? "NULL" : emp.bairro.ToString() )));
        parametros.Add(new Fields("cidade", (emp.cidade.ToString() == valorNull ? "NULL" : emp.cidade.ToString() )));
        parametros.Add(new Fields("cep", (emp.cep.ToString() == valorNull ? "NULL" : emp.cep.ToString() )));
        parametros.Add(new Fields("estado", (emp.estado.ToString() == valorNull ? "NULL" : emp.estado.ToString() )));
        parametros.Add(new Fields("representante", (emp.representante.ToString() == valorNull ? "NULL" : emp.representante.ToString() )));
        parametros.Add(new Fields("email", (emp.email.ToString() == valorNull ? "NULL" : emp.email.ToString() )));
        parametros.Add(new Fields("homePage", (emp.homePage.ToString() == valorNull ? "NULL" : emp.homePage.ToString() )));
        parametros.Add(new Fields("obs1", (emp.Obs1.ToString() == valorNull ? "NULL" : emp.Obs1.ToString() )));
        parametros.Add(new Fields("obs2", (emp.Obs2.ToString() == valorNull ? "NULL" : emp.Obs2.ToString() )));
        parametros.Add(new Fields("aceita_todos_seg", (emp.aceita_todos_seg.ToString() == valorNull ? "NULL" : emp.aceita_todos_seg.ToString() )));
        parametros.Add(new Fields("dataDebito", (emp.dataDebito.ToString() == valorNull ? "NULL" : emp.dataDebito.ToString() )));
        parametros.Add(new Fields("taxaBanco", (emp.taxaBanco.ToString() == valorNull ? "NULL" : emp.taxaBanco.ToString() )));
        parametros.Add(new Fields("valorTaxa", (emp.valorTaxa.ToString() == valorNull ? "NULL" : emp.valorTaxa.ToString() )));
        parametros.Add(new Fields("taxaJuros", (emp.taxaJuros.ToString() == valorNull ? "NULL" : emp.taxaJuros.ToString() )));
        parametros.Add(new Fields("multa", (emp.multa.ToString() == valorNull ? "NULL" : emp.multa.ToString() )));
        parametros.Add(new Fields("desc_func", (emp.Desc_func.ToString() == valorNull ? "NULL" : emp.Desc_func.ToString() )));
        parametros.Add(new Fields("repasseEmp", (emp.repasseEmp.ToString() == valorNull ? "NULL" : emp.repasseEmp.ToString() )));
        parametros.Add(new Fields("bloq_Ate_Pgto", (emp.Bloq_Ate_Pgto.ToString() == valorNull ? "NULL" : emp.Bloq_Ate_Pgto.ToString() )));
        parametros.Add(new Fields("obs3", (emp.Obs3.ToString() == valorNull ? "NULL" : emp.Obs3.ToString() )));
        parametros.Add(new Fields("pedeNf", (emp.pedeNf.ToString() == valorNull ? "NULL" : emp.pedeNf.ToString() )));
        parametros.Add(new Fields("pedeRec", (emp.pedeRec.ToString() == valorNull ? "NULL" : emp.pedeRec.ToString() )));
        parametros.Add(new Fields("aceitaParc", (emp.aceitaParc.ToString() == valorNull ? "NULL" : emp.aceitaParc.ToString() )));
        parametros.Add(new Fields("descontoEmp", (emp.descontoEmp.ToString() == valorNull ? "NULL" : emp.descontoEmp.ToString() )));
        parametros.Add(new Fields("usaCartaoProprio", (emp.usaCartaoProprio.ToString() == valorNull ? "NULL" : emp.usaCartaoProprio.ToString() )));
        parametros.Add(new Fields("cartaoIni", (emp.cartaoIni.ToString() == valorNull ? "NULL" : emp.cartaoIni.ToString() )));
        parametros.Add(new Fields("fidelidade", (emp.Fidelidade.ToString() == valorNull ? "NULL" : emp.Fidelidade.ToString() )));
        parametros.Add(new Fields("permite_venda_nome", (emp.Permite_venda_nome.ToString() == valorNull ? "NULL" : emp.Permite_venda_nome.ToString() )));
        parametros.Add(new Fields("encontroContas", (emp.encontroContas.ToString() == valorNull ? "NULL" : emp.encontroContas.ToString() )));
        parametros.Add(new Fields("solicitaProd", (emp.SolicitaProd.ToString() == valorNull ? "NULL" : emp.SolicitaProd.ToString() )));
        parametros.Add(new Fields("vendaNome", (emp.vendaNome.ToString() == valorNull ? "NULL" : emp.vendaNome.ToString() )));
        parametros.Add(new Fields("obsFechamento", (emp.obsFechamento.ToString() == valorNull ? "NULL" : emp.obsFechamento.ToString() )));
        parametros.Add(new Fields("limitePadrao", (emp.limitePadrao.ToString() == valorNull ? "NULL" : emp.limitePadrao.ToString() )));
        parametros.Add(new Fields("dataApagado", (emp.dataApagado.ToString() == valorNull ? "NULL" : emp.dataApagado.ToString() )));
        parametros.Add(new Fields("dataAlterado", (emp.dataAlterado.ToString() == valorNull ? "NULL" : emp.dataAlterado.ToString() )));
        parametros.Add(new Fields("operador", (emp.operador.ToString() == valorNull ? "NULL" : emp.operador.ToString() )));
        parametros.Add(new Fields("dataCadastro", (emp.dataCadastro.ToString() == valorNull ? "NULL" : emp.dataCadastro.ToString() )));
        parametros.Add(new Fields("operCadastro", (emp.operCadastro.ToString() == valorNull ? "NULL" : emp.operCadastro.ToString() )));
        parametros.Add(new Fields("vale_desconto", (emp.Vale_desconto.ToString() == valorNull ? "NULL" : emp.Vale_desconto.ToString() )));
        parametros.Add(new Fields("som_prod_prog", (emp.Som_prod_prog.ToString() == valorNull ? "NULL" : emp.Som_prod_prog.ToString() )));
        parametros.Add(new Fields("emiteNf", (emp.emiteNf.ToString() == valorNull ? "NULL" : emp.emiteNf.ToString() )));
        parametros.Add(new Fields("receita_sem_limite", (emp.Receita_sem_limite.ToString() == valorNull ? "NULL" : emp.Receita_sem_limite.ToString() )));
        parametros.Add(new Fields("complemento", (emp.complemento.ToString() == valorNull ? "NULL" : emp.complemento.ToString() )));
        parametros.Add(new Fields("usaCodImportacao", (emp.usaCodImportacao.ToString() == valorNull ? "NULL" : emp.usaCodImportacao.ToString() )));
        parametros.Add(new Fields("bandeira", (emp.Bandeira.ToString() == valorNull ? "NULL" : emp.Bandeira.ToString() )));
        parametros.Add(new Fields("naoGeraCartaoMenor", (emp.naoGeraCartaoMenor.ToString() == valorNull ? "NULL" : emp.naoGeraCartaoMenor.ToString() )));
        parametros.Add(new Fields("tipo_credito", (emp.Tipo_credito.ToString() == valorNull ? "NULL" : emp.Tipo_credito.ToString() )));
        parametros.Add(new Fields("diaRepasse", (emp.diaRepasse.ToString() == valorNull ? "NULL" : emp.diaRepasse.ToString() )));
        parametros.Add(new Fields("obriga_senha", (emp.Obriga_Senha.ToString() == valorNull ? "NULL" : emp.Obriga_Senha.ToString() )));
        parametros.Add(new Fields("qtdDigitosSenha", (emp.QtdDigitosSenha.ToString() == valorNull ? "NULL" : emp.QtdDigitosSenha.ToString() )));
        parametros.Add(new Fields("utilizaRecarga", (emp.utilizaRecarga.ToString() == valorNull ? "NULL" : emp.utilizaRecarga.ToString() )));
        parametros.Add(new Fields("responsavelFechamento", (emp.responsavelFechamento.ToString() == valorNull ? "NULL" : emp.responsavelFechamento.ToString())));
        return parametros;
      }

      public static MontadorSql setarParametrosEmpresa(string nomeTabela, Empresas emp, MontadorType mt)
      {
        MontadorSql montador = new MontadorSql(nomeTabela, mt);
        montador.AddField("empres_id", emp.Empres_id);
        montador.AddField("formaLimiteId", emp.formaLimiteId);
        montador.AddField("tipoCartao", emp.tipoCartao);
        montador.AddField("credenciado", emp.credenciado);
        montador.AddField("contrato", emp.contrato);
        montador.AddField("diaFechamento1", emp.diaFechamento1);
        montador.AddField("diaFechamento2", emp.diaFechamento2);
        montador.AddField("diaVencimento1", emp.diaVencimento1);
        montador.AddField("diaVencimento2", emp.diaVencimento2);
        montador.AddField("incCartPbm", emp.incCartPbm);
        montador.AddField("prog_desc", emp.Prog_desc);
        montador.AddField("nome", emp.Nome);
        montador.AddField("liberada", emp.Liberada);
        montador.AddField("fantasia", emp.Fantasia);
        montador.AddField("nomeCartao", emp.nomeCartao);
        montador.AddField("comissaoCred", emp.comissaoCred);
        montador.AddField("fatorRisco", emp.fatorRisco);
        montador.AddField("senha", emp.senha);
        montador.AddField("cgc", emp.cgc);
        montador.AddField("inscricaoEst", emp.inscricaoEst);
        montador.AddField("telefone1", emp.telefone1);
        montador.AddField("telefone2", emp.telefone2);
        montador.AddField("fax", emp.fax);
        montador.AddField("endereco", emp.endereco);
        montador.AddField("numero", emp.numero);
        montador.AddField("bairro", emp.bairro);
        montador.AddField("cidade", emp.cidade);
        montador.AddField("cep", emp.cep);
        montador.AddField("estado", emp.estado);
        montador.AddField("representante", emp.representante);
        montador.AddField("email", emp.email);
        montador.AddField("homePage", emp.homePage);
        montador.AddField("obs1", emp.Obs1);
        montador.AddField("obs2", emp.Obs2);
        montador.AddField("aceita_todos_seg", emp.aceita_todos_seg);
        montador.AddField("dataDebito", emp.dataDebito);
        montador.AddField("taxaBanco", emp.taxaBanco);
        montador.AddField("valorTaxa", emp.valorTaxa);
        montador.AddField("taxaJuros", emp.taxaJuros);
        montador.AddField("multa", emp.multa);
        montador.AddField("desc_func", emp.Desc_func);
        montador.AddField("repasseEmp", emp.repasseEmp);
        montador.AddField("bloq_Ate_Pgto", emp.Bloq_Ate_Pgto);
        montador.AddField("obs3", emp.Obs3);
        montador.AddField("pedeNf", emp.pedeNf);
        montador.AddField("pedeRec", emp.pedeRec);
        montador.AddField("aceitaParc", emp.aceitaParc);
        montador.AddField("descontoEmp", emp.descontoEmp);
        montador.AddField("usaCartaoProprio", emp.usaCartaoProprio);
        montador.AddField("cartaoIni", emp.cartaoIni);
        montador.AddField("fidelidade", emp.Fidelidade);
        montador.AddField("permite_venda_nome", emp.Permite_venda_nome);
        montador.AddField("encontroContas", emp.encontroContas);
        montador.AddField("solicitaProd", emp.SolicitaProd);
        montador.AddField("vendaNome", emp.vendaNome);
        montador.AddField("obsFechamento", emp.obsFechamento);
        montador.AddField("limitePadrao", emp.limitePadrao);
        montador.AddField("dataApagado", emp.dataApagado);
        montador.AddField("dataAlterado", emp.dataAlterado);
        montador.AddField("operador", emp.operador);
        montador.AddField("dataCadastro", emp.dataCadastro);
        montador.AddField("operCadastro", emp.operCadastro);
        montador.AddField("vale_desconto", emp.Vale_desconto);
        montador.AddField("som_prod_prog", emp.Som_prod_prog);
        montador.AddField("emiteNf", emp.emiteNf);
        montador.AddField("receita_sem_limite", emp.Receita_sem_limite);
        montador.AddField("complemento", emp.complemento);
        montador.AddField("usaCodImportacao", emp.usaCodImportacao);
        montador.AddField("bandeira", emp.Bandeira);
        montador.AddField("naoGeraCartaoMenor", emp.naoGeraCartaoMenor);
        montador.AddField("tipo_credito", emp.Tipo_credito);
        montador.AddField("diaRepasse", emp.diaRepasse);
        montador.AddField("obriga_senha", emp.Obriga_Senha);
        montador.AddField("qtdDigitosSenha", emp.QtdDigitosSenha);
        montador.AddField("utilizaRecarga", emp.utilizaRecarga);
        montador.AddField("responsavelFechamento", emp.responsavelFechamento);
        return montador;
      }

      public static bool addEmpresa(Empresas emp, string valorNull)
      {
        BD BD = new BD();
        MontadorSql montador = setarParametrosEmpresa("empresas",emp,MontadorType.Insert);
        bool retorno = false;
        try
        {
          SqlParamsList parametros = setarParametrosEmpresa(emp,valorNull);
          BD.ExecuteScalar(montador.GetSqlString(), parametros);
          parametros.Clear();
          parametros.Add(new Fields("empres_id",emp.Empres_id));
          retorno = Convert.ToInt32(BD.ExecuteScalar("select count(*) from empresas where empres_id = @empres_id",parametros)) == 1;
        }
        catch (Exception e)
        {
          throw new Exception("Erro ao incluir empresa.\n O erro foi: " + e.Message);
        }
        return retorno;
      }

      //Alterado para SqlServer
      public static bool GetNaoGerarCartaoParaMenor(int empres_id)
      {
        BD BD = new BD();
        return BD.ExecuteScalar("SELECT COALESCE(NAO_GERA_CARTAO_MENOR,'N') FROM EMPRESAS WHERE EMPRES_ID = " + empres_id.ToString(), null).ToString() == "S";
      }

      public static DataRow GetDadosLogin(int empres_id)
      {
         string sql = "SELECT empres_id, nome, COALESCE(senha,'1111') AS senha, prog_desc, inc_cart_pbm";
         sql += " FROM Empresas WHERE apagado <> 'S' AND empres_id = " + empres_id;

         BD BD = new BD();
         return BD.GetOneRow(sql, null);
      }

      public static int AlteraSenha(int empres_id, string senha)
      {
         BD BD = new BD();
         return BD.ExecuteNoQuery("UPDATE empresas SET senha = '" + senha + "' WHERE empres_id = " + empres_id, null);
      }

      public static string GetCodAdm()
      {
         string sql = "select COD_ADM_BIG from CONFIG";
         BD BD = new BD();
         return BD.GetDataTable(sql, null).Rows[0]["COD_ADM_BIG"].ToString();
      }

      //Alterado para SqlServer
      public static string GetFantasia()
      {
         BD BD = new BD();
         return BD.ExecuteScalar("SELECT top 1 fantasia FROM Administradora WHERE apagado <> 'S' ", null).ToString();
      }

      //Alterado para SqlServer
      public static DataRow GetDadosAdm()
      {
         BD BD = new BD();
         return BD.GetOneRow("SELECT * FROM Administradora WHERE apagado <> 'S' ", null);
      }

      //Alterado para SqlServer
      public static DataTable DatasFechamento(int empres_id)
      {
         BD BD = new BD();
         return BD.GetDataTable("SELECT data_fecha FROM dia_fecha WHERE empres_id = " + empres_id + " ORDER BY data_fecha", null);
      }

      //Alterado para SqlServer
      public static DateTime FechamentoAnterior(int empres_id)
      {
         BD BD = new BD();
         return (DateTime)BD.ExecuteScalar("SELECT MAX(data_fecha) FROM dia_fecha WHERE data_fecha < current_timestamp AND empres_id = " + empres_id, null);
      }

      public static DateTime ProximoFechamentoMenosUm(int empres_id)
      {
         //Pega a proxima data de fechamento depois da data atual
         BD BD = new BD();
         return (DateTime)BD.ExecuteScalar("SELECT MIN(data_fecha)-1 FROM dia_fecha WHERE data_fecha >= current_timestamp AND empres_id = " + empres_id, null);
      }

      //Alterado para SqlServer
      public static DateTime ProximoFechamento(int empres_id)
      {
         //Pega a proxima data de fechamento menos um dia depois da data atual
         BD BD = new BD();
         return (DateTime)BD.ExecuteScalar("SELECT MIN(data_fecha) FROM dia_fecha WHERE data_fecha >= current_timestamp AND empres_id = " + empres_id, null);
      }

      //Alterado para SqlServer
      public static DateTime GetVencimentoDoFechamento(int empres_id, DateTime data_fecha)
      {
         BD BD = new BD();
         return (DateTime)BD.ExecuteScalar("SELECT data_venc FROM dia_fecha WHERE data_fecha = '" + data_fecha.ToString("dd/MM/yyyy") + "' AND empres_id = " + empres_id, null);
      }

      //Alterado para SqlServer
      public static DataTable GetGrupos(int empres_id)
      {
         string sql = "SELECT grupo_conv_emp_id, descricao";
         sql += " FROM Grupo_Conv_Emp";
         sql += " WHERE empres_id=" + empres_id;

         BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }

      public static string GetGrupo(int empres_id, string grupo)
      {
         string sql = "SELECT descricao";
         sql += " FROM Grupo_Conv_Emp";
         sql += " WHERE empres_id=" + empres_id;
         sql += " AND grupo_conv_emp_id=" + grupo;

         BD BD = new BD();
         return BD.ExecuteScalar(sql, null).ToString();
      }


      public static DataTable GetVerificaLancAlim(int empres_id)
      {
          BD BD = new BD();
          return BD.GetDataTable("SELECT * FROM ALIMENTACAO_RENOVACAO WHERE EMPRES_ID = " + empres_id.ToString(), null);
      }

      public static int GeraRenovacaoID()
      {
          BD BD = new BD();
          return Convert.ToInt32(BD.ExecuteScalar(" SELECT NEXT VALUE FOR SRENOVACAO_ID", null).ToString());
      }

      public static bool GetInserirAliRenovacao(int renovacaoID, int empres_id, string dataRenovacao, string tipoCredito, string pOperador)
      {
          MontadorSql mont2 = new MontadorSql("alimentacao_renovacao", MontadorType.Insert);
          mont2.AddField("renovacao_id", renovacaoID);
          mont2.AddField("empres_id", empres_id);
          mont2.AddField("data_renovacao", dataRenovacao);
          mont2.AddField("tipo_credito", tipoCredito);
          
          BD BD2 = new BD();
          int incluiu = BD2.ExecuteNoQuery(mont2.GetSqlString(), mont2.GetParams());
          if (incluiu == 1)
          {
              int logID = Log.GeraLogID();
              Log.GravaLog(logID, "FCadEmp", "DATA_RENOVACAO", "", dataRenovacao, pOperador, "Inclusão", "Cadastro de Alimentação Renovacao", Convert.ToString(empres_id), "Empresa ID: " + empres_id, "",0);
              return true;
          }
          else
              return false;

      }

      public static int GetUpDataRenovacao(int empres_id, string dataRenovacao, int renovacaoID, string tipo)
      {
          BD BD = new BD();
          return BD.ExecuteNoQuery("UPDATE alimentacao_renovacao SET data_renovacao = '" + dataRenovacao + "', tipo_credito = '" + tipo + "' WHERE renovacao_id = " + renovacaoID + " and empres_id = " + empres_id, null);
      }


      #endregion
   }
}
