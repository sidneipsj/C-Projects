using System;
using System.Collections.Generic;
using System.Text;
using SQLHelperv2;
using Negocio;
using System.Data;
using Misc;

namespace DAL
{
    /// <summary>
    /// Classe que Gerecia dados da tabela Credenciados(Farmacia)
    /// </summary>
    public class DALCredenciado
    {
        //Alterado para SqlServer
        public static int getCodAcessoPorCredId(int id)
        {
            SqlParamsList ps = new SqlParamsList();
            ps.Add(new Fields("@id", id));
            BD BD = new BD();
            return BD.ExecuteScalar(-1, "select codacesso from credenciados where cred_id = @id", ps);
        }

        public static int getCredIdFromCodAcesso(int codAcesso)
        {
            SqlParamsList ps = new SqlParamsList();
            ps.Add(new Fields("@codAcesso", codAcesso));
            BD BD = new BD();
            return BD.ExecuteScalar(-1, "select cred_id from credenciados where codacesso = @codAcesso", ps);
        }

        public static int getCredIdFromCodAcessoESenha(int codAcesso, string senha)
        {
            SqlParamsList ps = new SqlParamsList();
            ps.Add(new Fields("@codAcesso", codAcesso));
            ps.Add(new Fields("@senha", senha));
            BD BD = new BD();
            return BD.ExecuteScalar(-1, "select cred_id from credenciados where codacesso = @codAcesso and senha = @senha", ps);
        }

        //Alterado para SqlServer
        public static String getEmail(int credID)
        {
            SqlParamsList ps = new SqlParamsList();
            ps.Add(new Fields("@cred_id", credID));
            BD BD = new BD();
            return BD.ExecuteScalar("", "select email from credenciados where cred_id = @cred_id", ps).ToString();
        }

        public static String getCNPJ(int credID)
        {
            SqlParamsList ps = new SqlParamsList();
            ps.Add(new Fields("@cred_id", credID));
            BD BD = new BD();
            return BD.ExecuteScalar("", "select CGC from credenciados where cred_id = @cred_id", ps).ToString();
        }

        public static bool VerifSenha(Credenciados cred)
        {
            BD BD = new BD();
            bool retorno = false;
            SafeDataReader dr = BD.GetDataReader("Select SENHA from credenciados where APAGADO <> 'S' and CRED_ID = " + cred.Cred_id, null);
            try
            {
                if (dr.Read())
                {
                    cred.Senha = dr.GetString(0);
                    retorno = true;
                }
            }
            finally
            {
                dr.Close();
            }
            return retorno;
        }

        public static bool GetObrigadoPassarSenhaConveniado(Credenciados cred)
        {
            BD BD = new BD();
            bool retorno = false;
            SafeDataReader dr = BD.GetDataReader("select coalesce(obrigar_senha,'N') as obrigar_senha FROM EMP_CRED_OBRIGA_SENHA where empres_id = " + cred.Empresa.Empres_id.ToString() + " estab_id = " + cred.Cred_id.ToString(), null);
            try
            {
                if (dr.Read())
                {
                    retorno = dr.GetString(0) == "S";
                }
            }
            finally
            {
                dr.Close();
            }
            return retorno;


        }

        public static bool Update(Credenciados cred)
        {
            BD BD = new BD();
            if (BD.ExecuteNoQuery("UPDATE Credenciados SET senha = '" + cred.Senha + "' WHERE cred_id = " + cred.Cred_id, null) == 1)
                return true;
            return false;
        }

        public static bool Update_BE(Credenciados cred)
        {
            BD BD = new BD();
            if (BD.ExecuteNoQuery("UPDATE credenciados_bem_estar SET senha = '" + cred.Senha + "' WHERE CRED_BE_ID = " + cred.Cred_id, null) == 1)
                return true;
            return false;
        }

        public static Credenciados Get_Id_Senha_Nome(int codacesso)
        {
            BD BD = new BD();
            SafeDataReader dr = BD.GetDataReader("SELECT cred_id, senha, nome  FROM Credenciados WHERE apagado <> 'S' AND codacesso =  " + codacesso, null);
            Credenciados cred = new Credenciados();

            try
            {
                if (dr.Read())
                {
                    cred.Senha = dr.GetString("senha");
                    cred.Cred_id = dr.GetInt32("cred_id");
                    cred.Nome = dr.GetString("nome");
                }
            }
            finally
            {
                dr.Close();
            }

            return cred;
        }

        public static Credenciados Get_Id_Senha_Nome_BE(int codacesso)
        {
            BD BD = new BD();
            SafeDataReader dr = BD.GetDataReader("SELECT CRED_BE_ID, senha, nome  FROM credenciados_bem_estar WHERE apagado <> 'S' AND codacesso =  " + codacesso, null);
            Credenciados cred = new Credenciados();

            try
            {
                if (dr.Read())
                {
                    cred.Senha = dr.GetString("senha");
                    cred.Cred_id = dr.GetInt32("CRED_BE_ID");
                    cred.Nome = dr.GetString("nome");
                }
            }
            finally
            {
                dr.Close();
            }

            return cred;
        }

        public static bool LogarCredenciado(string codacesso, string senha, out int cred_id)
        {
            bool r = false;
            cred_id = 0;
            BD BD = new BD();
            SafeDataReader dr = BD.GetDataReader("SELECT senha, cred_id FROM Credenciados WHERE codacesso = " + codacesso, null);
            try
            {
                if (dr.Read())
                {
                    netUtil.Funcoes f = new netUtil.Funcoes();
                    if (f.Crypt("D", dr.GetString(0), "BIGCOMPRAS") == senha)
                    {
                        r = true;
                        cred_id = dr.GetInt32(1);
                    }
                }
            }
            finally
            {
                dr.Close();
            }

            return r;
        }

        //Alterado para SqlServer
        public static string GetExibiDIRF(int cred_id)
        {
            string sql = "SELECT coalesce(exibir_dirf,'N') FROM credenciados WHERE apagado <> 'S' AND cred_id = @credID";
            SqlParamsList ps = new SqlParamsList();
            ps.Add(new Fields("@credID", cred_id));
            BD BD = new BD();
            return BD.ExecuteScalar(sql, ps).ToString();

        }

        //Alterado para SqlServer
        public static Credenciados GetCodAcesso_Senha_Nome(int cred_id)
        {
            try
            {
                BD BD = new BD();
                SafeDataReader dr = BD.GetDataReader("SELECT codacesso, senha, nome FROM Credenciados WHERE apagado <> 'S' AND cred_id =" + cred_id, null);

                Credenciados cred = new Credenciados();

                try
                {
                    if (dr.Read())
                    {
                        cred.Senha = dr.GetString("senha");
                        cred.Codacesso = dr.GetInt32("codacesso");
                        cred.Nome = dr.GetString("nome");
                    }
                }
                finally
                {
                    dr.Close();
                }
                return cred;
            }
            catch (Exception ex)
            {
                throw new Exception("Estabelecimento não encontrado para o id: " + cred_id);
            }
        }

        public static Credenciados GetCodAcesso_Senha_Nome_BE(int cred_id)
        {
            try
            {
                BD BD = new BD();
                SafeDataReader dr = BD.GetDataReader("SELECT codacesso, senha, nome FROM credenciados_bem_estar WHERE apagado <> 'S' AND CRED_BE_ID = " + cred_id, null);

                Credenciados cred = new Credenciados();

                try
                {
                    if (dr.Read())
                    {
                        cred.Senha = dr.GetString("senha");
                        cred.Codacesso = dr.GetInt32("codacesso");
                        cred.Nome = dr.GetString("nome");
                    }
                }
                finally
                {
                    dr.Close();
                }
                return cred;
            }
            catch (Exception ex)
            {
                throw new Exception("Estabelecimento não encontrado para o id: " + cred_id);
            }
        }


        #region Relatórios WebEstabelecimentos

        // Pega informacoes do proprio Credenciado (Farmacia)
        // Carrega um novo DataTable para preencher os dados do Credenciado no Relatorio de Valores a Receber</returns>
        public static DataTable aRecebFornecedor(int cred_id, string pExibirPagto, string faturas)
        {
            StringBuilder sql = new StringBuilder();
            sql.Append(" select cred_id, fornecedor, ");
            sql.Append(" sum(valor_bruto)  valor_bruto, ");
            sql.Append(" sum(valor_comiss) valor_comiss, ");
            sql.Append(" sum(valor_comiss)/sum(valor_bruto) * 100 per_comiss, ");
            sql.Append(" coalesce((select sum(valor) from get_descontoscred(cred_id,coalesce(sum(valor_bruto),0))),0) as desconto ");
            sql.Append(" from ");
            sql.Append(" ( select cred.cred_id, cred.nome as fornecedor, ");
            sql.Append("   sum(cc.debito-cc.credito) as valor_bruto, ");
            sql.Append("   sum(cc.debito-cc.credito)/100*(coalesce(cred_emp_lib.comissao,cred.comissao,0)) as valor_comiss ");
            sql.Append("   from fatura fat ");
            sql.Append("   join contacorrente cc on cc.fatura_id = fat.fatura_id and coalesce(cc.pagamento_cred_id,0) = 0 ");
            sql.Append("   and coalesce(cc.baixa_credenciado,'N') = 'N' ");
            if (pExibirPagto == "S")
            {
                sql.Append(" join rel_fat_pag_cred rel on rel.fatura_id = fat.fatura_id ");
                sql.Append(" and rel.cred_id = cc.cred_id and rel.exibir = 'S' ");
            }
            sql.Append("   join credenciados cred on cred.cred_id = cc.cred_id and cred.paga_cred_por_id = 1 ");
            sql.Append("   join conveniados conv on conv.conv_id = cc.conv_id ");
            sql.Append("   left join cred_emp_lib on cred_emp_lib.cred_id = cc.cred_id ");
            sql.Append("   and cred_emp_lib.empres_id = conv.empres_id ");
            sql.Append("   where coalesce(fat.apagado,'N') <> 'S' ");
            sql.Append(" and ((coalesce(fat.baixada,'N') = 'S') or (coalesce(fat.pre_baixa,'N') = 'S'))");
            sql.Append("   and (cred.CRED_ID = " + cred_id + ") ");

            if (!string.IsNullOrEmpty(faturas))
                sql.Append("   and cc.fatura_id in (" + faturas + ")");

            sql.Append("   group by  cred.comissao, cred.nome, cred.cred_id, cred_emp_lib.comissao ");
            sql.Append(" ) ");
            sql.Append(" group by cred_id, fornecedor ");

            BD BD = new BD();
            DataTable dtFor = BD.GetDataTable(sql.ToString(), null);
            return dtFor;
        }

        // Pega informacoes do proprio Credenciado (Farmacia) dentro da data indicada
        // Carrega um novo DataTable para preencher os dados do Credenciado no Relatorio de Valores Recebidos</returns>
        public static DataTable RecebidoFornecedor(Extratos ext, int cred_id)
        {
            string sql = " SELECT pag.pagamento_cred_id, cred.cred_id, cred.nome AS fornecedor,";
            sql += " (pag.per_comissao + pag.per_comissao_r) AS per_comiss,";
            sql += " (pag.valor_comissao_r + pag.valor_comissao) AS valor_comissao, ";
            sql += " pag.valor_total AS valor_bruto ";
            sql += " FROM Pagamento_cred pag";
            sql += " JOIN Credenciados cred ON cred.cred_id = pag.cred_id";
            sql += " WHERE pag.apagado <> 'S'";
            sql += " AND cred.cred_id = " + cred_id;

            if (!ext.TodasEmpresas)
            {
                string emps = String.Empty;
                foreach (string e in ext.EmpresasMarcadas.Keys)
                    emps += "," + e;
                emps = emps.Substring(1);//ignorar a primeira virgula.

                sql += " AND (pag.pagamento_cred_id IN (" + emps + ")) ";
            }
            else
            {
                sql += " AND data_pgto BETWEEN '" + ext.DataIni.ToString("MM/dd/yyyy 00:00:00") + "'";
                sql += " AND '" + ext.DataFim.ToString("MM/dd/yyyy 23:59:59") + "'";
            }

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        // Pega as informações dos Pagamentos do Fornecedor
        // Datatable para preencher o grid na tela de visualizacao do Relatorio de Valores Recebidos</returns>
        public static DataTable RecebidoFornecedorGrid(Extratos ext, int cred_id)
        {
            string sql = " SELECT pag.pagamento_cred_id, pag.data_hora,pag.valor_total AS valor_bruto,";
            sql += " (pag.per_comissao + pag.per_comissao_r) AS per_comiss,";
            sql += " (pag.valor_comissao_r + pag.valor_comissao) AS valor_comissao, ";
            sql += " ((pag.taxas_fixas + pag.taxas_variaveis) - pag.repasse) AS descontos,";
            sql += " pag.valor_pago, pagapor.descricao, pag.operador";
            sql += " FROM Pagamento_cred pag";
            sql += " JOIN Credenciados cred ON cred.cred_id = pag.cred_id";
            sql += " JOIN Paga_cred_por pagapor ON pagapor.paga_cred_por_id = pag.paga_cred_por_id";
            sql += " WHERE pag.apagado <> 'S'";
            sql += " AND cred.cred_id = " + cred_id;
            sql += " AND data_pgto BETWEEN '" + ext.DataIni.ToString("MM/dd/yyyy 00:00:00") + "'";
            sql += " AND '" + ext.DataFim.ToString("MM/dd/yyyy 23:59:59") + "'";

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable GetSaldoDirf(int cred_id, int ano)
        {
            BD BD = new BD();
            return BD.GetDataTable("SELECT ANO, MES, COALESCE(VALOR,0.00) VALOR FROM DIRF WHERE CRED_ID = " + cred_id + " AND ANO = " + ano, null);
        }

        public static int GetVerificaDirf(int cred_id, int ano)
        {
            BD BD = new BD();
            return BD.ExecuteScalar(-1, " select cred_id from dirf where cred_id = " + cred_id + " and ano = " + ano, null);
        }




        public static DataTable RelatorioVendasCantinex(int cred_id, string data1, string data2)
        {
            SqlParamsList ps = new SqlParamsList();
            ps.Add(new Fields("@dtInicial", data1));
            ps.Add(new Fields("@dtFinal", data2));
            ps.Add(new Fields("@credID", cred_id));

            string sql;
            sql = " SELECT CONVERT(VARCHAR(10),CC.DATAVENDA,103) AS DATA,  CART.NOME,(concat(CAST(cc.autorizacao_id AS varchar(10)),"
                + " (CASE WHEN cc.digito < 10 THEN '0'+ CAST(cc.digito AS char(1)) ELSE"
                + " CAST(cc.digito AS char(2)) end))) AS AUTORIZACAO,"
                + " SUM(CC.DEBITO - CC.CREDITO) AS SALDO"
                + " FROM CARTOES CART"
                + " INNER JOIN CONTACORRENTE CC ON CC.CONV_ID = CART.CONV_ID"
                + " WHERE CC.DATAVENDA BETWEEN @dtInicial AND @dtFinal and CC.CARTAO_ID = @credID"
                + " GROUP BY CART.NOME, CC.AUTORIZACAO_ID, CC.DIGITO, CC.DATAVENDA"
                + " ORDER BY CC.DATAVENDA, CART.NOME";

            BD BD = new BD();
            return BD.GetDataTable(sql, ps);
        }


        //Alterado para SqlServer
        public static DataTable RelatorioRepasse(int cred_id, int seg_id, DateTime data1, DateTime data2)
        {
            string sql;

            if (seg_id != 14 && seg_id != 39)
            {
                sql = "SELECT TITULAR, EMPRESA, NOTA_FISCAL, VALOR, TRANS_ID FROM PAGAMENTOS_REPASSE WHERE CRED_ID = " + cred_id + " AND DATA = '"
                + data1.ToString("dd.MM.yyyy 00:00:00") + "' ORDER BY  EMPRESA,TITULAR ";
            }
            else
            {
                sql = " SELECT C.TITULAR, D.NOME AS EMPRESA, A.NF AS NOTA_FISCAL, A.TRANS_ID, (A.DEBITO - A.CREDITO) AS VALOR FROM CONTACORRENTE A"
                      + " JOIN CONVENIADOS C ON C.CONV_ID = A.CONV_ID"
                      + " JOIN EMPRESAS D ON D.EMPRES_ID = C.EMPRES_ID"
                      + " WHERE A.DATA BETWEEN '" + data1.ToString("dd.MM.yyyy 00:00:00") + "' AND '" + data2.ToString("dd.MM.yyyy 23:59:59") + "' AND"
                      + " A.CRED_ID = " + cred_id
                      + " ORDER BY D.NOME, C.TITULAR";
            }

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        //Alterado para SqlServer
        public static DataTable PeriodoRepasse(int seg_id, DateTime data, int dias)
        {
            string sql = "SELECT DATA_INICIO, DATA_FIM FROM CALENDARIO_REPASSE WHERE SEG_ID = " + seg_id + " AND PAGAMENTO = '" + data.ToString("dd.MM.yyyy") + "' AND DIAS = " + dias;

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        //Alterado para SqlServer
        public static DataTable DatasRepasse(int cred_id, int seg_id, int dias = 0)
        {
            string sql;
            if (seg_id != 14 && seg_id != 39)
            {
                sql = "SELECT DISTINCT DATA FROM PAGAMENTOS_REPASSE WHERE CRED_ID = " + cred_id + " ORDER BY DATA";
            }
            else
            {
                sql = "SELECT PAGAMENTO FROM CALENDARIO_REPASSE WHERE SEG_ID = " + seg_id + " AND PAGAMENTO < CURRENT_TIMESTAMP AND DIAS = " + dias + " ORDER BY PAGAMENTO";
            }

            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable DatasRepassePagamentosBaixados(int cred_id)
        {
            string sql;
            sql = "SELECT PAGAMENTO_CRED_ID, "+
                  "(SELECT DISTINCT CONVERT(varchar,DATA_COMPENSACAO,103)) as data_compensacao, "+
                  "(SELECT DISTINCT CONVERT(varchar,DATA_PGTO,103)) as data_pgto, "+
                  "VALOR_TOTAL, "+
	              "VALOR_COMISSAO AS TAXAS_ADM, "+
	              "VALOR_PAGO "+
                  "FROM PAGAMENTO_CRED WHERE CRED_ID = " + cred_id + " AND DATA_PGTO >= '01/03/2015'";
            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        //public static DataTable RelatorioRepasseBaixados(int cred_id, string dataIni, string dataFin, int pagaCredPorId)
        //{
        //    StringBuilder sql = new StringBuilder();
        //    String concat="";
        //    sql.Append("SELECT * FROM (");
        //    sql.Append("SELECT CC.PAGAMENTO_CRED_ID, ");
        //    sql.Append("COALESCE(SUM(CC.DEBITO - CC.CREDITO), 0) AS BRUTO, ");
        //    if (pagaCredPorId == 2)
        //    {
        //        sql.Append("cast(((coalesce(cred.TX_DVV,0.0) * 41)/100) as float) as TX_DVV, ");
        //    }
        //    else if (pagaCredPorId == 10) 
        //    {
        //        sql.Append("cast(((coalesce(cred.TX_DVV,0.0) * 30)/100)as float) as TX_DVV, ");
        //    }
        //    sql.Append("(coalesce((sum(CC.debito - CC.credito))*(cred.COMISSAO/100),0)) as COMISSAO_ADM, ");
            
        //   concat = "((COALESCE(SUM(CC.DEBITO - CC.CREDITO),0))"+
        //   "-"+
        //   "((coalesce((sum(CC.debito - CC.credito))*(cred.COMISSAO/100),0))+(SELECT COALESCE(SUM(t.valor),0)"+
        //   "FROM TAXAS t, rel_taxa_cred rtc WHERE t.taxa_id = rtc.TAXA_ID and rtc.cred_id = cred.CRED_ID))"+
        //   ") AS LIQUIDO ";
        //    sql.Append(concat);
        //    //LIMPANDO A VARIÁVEL AUXILIAR DE CONCATENAÇÃO
        //    concat = "";
            
        //    sql.Append("FROM CREDENCIADOS cred ");
        //    sql.Append("LEFT JOIN contacorrente cc  ON CC.CRED_ID = cred.CRED_ID AND cc.data between '" + dataIni + "' and '" + dataFin + "' ");

        //    sql.Append("AND cc.BAIXA_CREDENCIADO = 'S' ");
            
        //    if (pagaCredPorId == 7) 
        //        sql.Append("INNER JOIN SEGMENTOS seg ON SEG.SEG_ID = CRED.SEG_ID and CRED.SEG_ID = 39 ");
        //    else if (pagaCredPorId == 8) 
        //       sql.Append("INNER JOIN SEGMENTOS seg ON SEG.SEG_ID = CRED.SEG_ID AND CRED.SEG_ID = 39 ");
        //    else if (pagaCredPorId == 4) 
        //    {
        //        sql.Append("INNER JOIN SEGMENTOS seg ON SEG.SEG_ID = CRED.SEG_ID AND CRED.SEG_ID <> 39 and CRED.SEG_ID <> 14 AND cred.PAGA_CRED_POR_ID in (4,1) where cred.APAGADO = 'N' ");
        //    }
        //    else
        //        sql.Append("WHERE cred.PAGA_CRED_POR_ID = "+pagaCredPorId+" AND cred.APAGADO = 'N' ");
            
        //    sql.Append("AND cred.CRED_ID = "+cred_id);
      
        //    sql.Append(" GROUP BY CC.PAGAMENTO_CRED_ID, cred.CRED_ID,cc.BAIXA_CREDENCIADO,CRED.NOME,cred.diafechamento1,cred.vencimento1,cred.CORRENTISTA,cred.cgc, cred.CONTACORRENTE, cred.AGENCIA,cred.COMISSAO,cred.BANCO,cred.TX_DVV");
        //    sql.Append(" )A WHERE A.BRUTO <> 0;");
        
        //    BD BD = new BD();
        //    return BD.GetDataTable(Convert.ToString(sql), null);
        //}

        public static DataTable RelatorioRepasseBaixados(int cred_id, string dataCompensacao, int pagaCredPorId)
        {
            string sql;
            sql = "SELECT PAGAMENTO_CRED_ID, " +
                  "(SELECT DISTINCT CONVERT(varchar,DATA_COMPENSACAO,103)) as data_compensacao, " +
                  "(SELECT DISTINCT CONVERT(varchar,DATA_PGTO,103)) as data_pgto, " +
                  "VALOR_TOTAL AS BRUTO, " +
                  "VALOR_COMISSAO AS TAXAS_ADM, " +
                  "VALOR_PAGO AS LIQUIDO, " +
                  "COALESCE((SELECT SUM(T.VALOR) FROM TAXAS T LEFT JOIN TAXAS_REPASSE TR ON TR.TAXA_ID = T.TAXA_ID WHERE TR.CRED_ID = "+cred_id+" AND DT_DESCONTO = '"+dataCompensacao+"'),0) AS TAXAS_EXTRAS " +
                  "FROM PAGAMENTO_CRED WHERE CRED_ID = " + cred_id + " AND DATA_COMPENSACAO = '"+dataCompensacao+"'";
            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static DataTable getTaxasById(int cred_id, string dataCompensacao)
        {
            string sql;
            sql = "SELECT COALESCE(T.DESCRICAO,'') as DESCRICAO, COALESCE(T.VALOR,0) as VALOR FROM TAXAS T " +
                  "LEFT JOIN TAXAS_REPASSE TR ON TR.TAXA_ID = T.TAXA_ID " + 
                  "WHERE TR.CRED_ID = "+cred_id+" AND DT_DESCONTO = '"+dataCompensacao+"'";
            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static int PagamentoCredPor(int cred_id)
        {
            string sql;
            sql = "Select paga_cred_por_id FROM credenciados WHERE cred_id = " + cred_id;
            BD bd = new BD();
            return Convert.ToInt32(bd.ExecuteScalar(sql, null));
        }


        //Alterado para SqlServer
        public static int PagaCred(int cred_id)
        {
            SqlParamsList ps = new SqlParamsList();
            ps.Add(new Fields("@cred_id", cred_id));
            BD BD = new BD();
            return BD.ExecuteScalar(-1, "select paga_cred_por_id from credenciados where cred_id = @cred_id", ps);
        }

        #endregion


        //Inclusão atualizador 07/2013 - Ariane//
        public static DataSet DadosCredenciado(int cred_id)
        {
            string sql = "SELECT * FROM CREDENCIADOS WHERE CRED_ID = " + cred_id;

            BD BD = new BD();
            return BD.GetDataSet(sql, null);
        }

        //Alterado para SqlServer
        public static string VerificaSeg(int cred_id)
        {
            BD BD = new BD();
            return (BD.ExecuteScalar("SELECT SEG_ID FROM CREDENCIADOS WHERE CRED_ID = " + cred_id, null).ToString());
        }

        //Ariane - Mudanca no NAVS - 05/2015//
        public static bool GetValidaCantinex(int pos_serial)
        {
            BD BD = new BD();
            bool retorno = false;
            string sql = " SELECT CRED.SEG_ID FROM POS P"
                  + " INNER JOIN CREDENCIADOS CRED ON CRED.CRED_ID = P.CRED_ID"
                  + " INNER JOIN CONFIG CON ON CON.SEG_ID_CANTINA = CRED.SEG_ID  WHERE P.POS_SERIAL_NUMBER = " + pos_serial;

            SafeDataReader dr = BD.GetDataReader(sql, null);
            try
            {
                if (dr.Read())
                {
                    if (dr.GetObject(0) != "")
                        retorno = true;
                }
            }
            finally
            {
                dr.Close();
            }

            return retorno;
        }

        public static int getCodAcessoPorSerial(string serial)
        {
            SqlParamsList ps = new SqlParamsList();
            ps.Add(new Fields("@serial", serial));
            BD BD = new BD();
            string sql = " SELECT CRED.CODACESSO FROM POS P"
                  + " INNER JOIN CREDENCIADOS CRED ON CRED.CRED_ID = P.CRED_ID  WHERE P.POS_SERIAL_NUMBER = @serial";

            return BD.ExecuteScalar(-1, sql, ps);
        }

        //Alterado para SqlServer
        public static bool UtilizaAutorizador(int cred_id)
        {
            BD BD = new BD();
            bool retorno = false;
            SafeDataReader dr = BD.GetDataReader("SELECT UTILIZA_AUTORIZADOR FROM CREDENCIADOS WHERE CRED_ID = " + cred_id, null);
            try
            {
                if (dr.Read())
                {
                    retorno = dr.GetString(0) == "S";
                }
            }
            finally
            {
                dr.Close();
            }

            return retorno;
        }

        //Alterado para SqlServer
        public static int CredID(int pos_serial_number)
        {
            string sql = " SELECT CRED_ID FROM POS WHERE POS_SERIAL_NUMBER = '" + pos_serial_number + "'";
            BD BD = new BD();
            int cred_id = (int)BD.ExecuteScalar(sql, null);

            return cred_id;
        }

        //Alterado para SqlServer
        public static bool UtilizaComanda(int cred_id)
        {
            BD BD = new BD();
            bool retorno = false;
            SafeDataReader dr = BD.GetDataReader("SELECT UTILIZA_COMANDA FROM CREDENCIADOS WHERE CRED_ID = " + cred_id, null);

            try
            {
                if (dr.Read())
                    retorno = dr.GetString(0) == "S";
            }
            finally
            {
                dr.Close();
            }

            return retorno;
        }

        //Alterado para SqlServer
        public static bool UtilizaRecarga(int cred_id)
        {
            BD BD = new BD();
            bool retorno = false;
            SafeDataReader dr = BD.GetDataReader("SELECT UTILIZA_RECARGA FROM CREDENCIADOS WHERE CRED_ID = " + cred_id, null);
            try
            {
                if (dr.Read())
                {
                    retorno = dr.GetString(0) == "S";
                }
            }
            finally
            {
                dr.Close();
            }
            return retorno;
        }

        public static string BuscaCodAcesso(string cnpj)
        {
            BD BD = new BD();
            return (BD.ExecuteScalar("SELECT CODACESSO FROM CREDENCIADOS WHERE CGC = '" + cnpj + "'", null).ToString());
        }


        public static DataTable DadosVendaCartaoNsu(string trans_id)
        {
            string sql;

            sql = "SELECT CARTAO, NSU FROM TRANSACOES WHERE TRANS_ID = " + trans_id;
            BD BD = new BD();
            return BD.GetDataTable(sql, null);
        }

        public static bool GetValidaCantinexPorCred(int credId)
        {
            BD BD = new BD();
            bool retorno = false;
            string sql = " SELECT SEG_ID_CANTINA FROM "
                  + "  CONFIG CON "
                  + "  INNER JOIN CREDENCIADOS C ON C.SEG_ID = CON.SEG_ID_CANTINA "
                  + "  where C.CRED_ID = " + credId;

            SafeDataReader dr = BD.GetDataReader(sql, null);
            try
            {
                if (dr.Read())
                {
                    if (dr.GetObject(0) != "")
                        retorno = true;
                }
            }
            finally
            {
                dr.Close();
            }

            return retorno;
        }


        public static DataTable GetEmailBemEstar()
        {
            StringBuilder sql = new StringBuilder();
            sql.Append("SELECT * FROM EMAIL"); 

            BD BD = new BD();
            DataTable dtFor = BD.GetDataTable(sql.ToString(), null);
            return dtFor;
        }

        public static void DelEmail(string codacesso)
        {
            BD BD = new BD();
            BD.ExecuteNoQuery(" DELETE FROM EMAIL WHERE codacesso = " + codacesso, null);
        }
    }
}
