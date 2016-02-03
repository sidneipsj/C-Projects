using System;
using System.Collections.Generic;
using SQLHelperv2;
using System.Data;
using Negocio;

namespace DAL
{
   public class DALExtrato
   {  
      #region WebEmpresas
      public static DataTable GetFechamentos(bool vzerado, bool baixados, string tipoext, string nfentreg,
                                             string datafecha, string dataini, string datafim, int empres_id,
                                             string chapa, string cartao, string nome, string status, string grupo)
      {
         string sql = " select CONVENIADOS.CONV_ID, CONVENIADOS.CHAPA, CONVENIADOS.TITULAR, CONVENIADOS.GRUPO_CONV_EMP, ";
         sql += " CONTACORRENTE.DATA_FECHA_EMP, ";
         sql += " CONTACORRENTE.DATA_VENC_EMP, ";
         sql += " coalesce(CONTACORRENTE.RECEITA,'N') AS RECEITA, ";
         sql += " coalesce(sum(CONTACORRENTE.DEBITO - CONTACORRENTE.CREDITO),0) as VALOR ";
         sql += " from CONVENIADOS  ";
         sql += (vzerado ? " left " : "") + "join CONTACORRENTE on ( CONVENIADOS.CONV_ID = CONTACORRENTE.CONV_ID ) ";

         #region Valores Pagos         
         if (!baixados)
            sql += " and (COALESCE(CONTACORRENTE.BAIXA_CONVENIADO,'N') <> 'S')";
         #endregion

         #region Tipo de Autorizacao
         switch (nfentreg)
         {
            case "S": sql += " and (COALESCE(CONTACORRENTE.ENTREG_NF,'N') = 'S') "; break;
            case "N": sql += " and (COALESCE(CONTACORRENTE.ENTREG_NF,'N') = 'N') "; break;
         }
         #endregion

         #region Tipo Extrato

         if (tipoext == "F") //Fechamento
            sql += " and (CONTACORRENTE.DATA_FECHA_EMP = '" + Convert.ToDateTime(datafecha).ToString("MM/dd/yyyy") + "')";
         else if (tipoext == "P") //extrato periodo
            sql += " and (CONTACORRENTE.DATA between '" + Convert.ToDateTime(dataini).ToString("MM/dd/yyyy") + "' and '" + Convert.ToDateTime(datafim).ToString("MM/dd/yyyy") + "')";
         #endregion

         sql += " where (CONVENIADOS.APAGADO <> 'S')";
         sql += " and (CONVENIADOS.EMPRES_ID = " + empres_id + ")";

         #region Conveniado

         netUtil.Funcoes funcoes = new netUtil.Funcoes();

         if (grupo != "0")
            sql += " and conveniados.grupo_conv_emp=" + grupo;

         if (funcoes.SoNumero(chapa) != "")
            sql += " and conveniados.chapa = " + funcoes.SoNumero(chapa);
         else
            if (funcoes.SoNumero(cartao) != "") //procura pelo cartao
            {
               if ((cartao.Length == 11) && (Convert.ToInt32(cartao.Substring(9, 2)) == funcoes.DigitoCartao(Convert.ToInt32(cartao.Substring(0, 9)))))
                  sql += " and conveniados.conv_id = (SELECT conv_id FROM cartoes WHERE codigo = " + cartao.Substring(0, 9) + ")";
               else //cartao de importação
                  sql += " and conveniados.conv_id = (SELECT FIRST 1 conv_id FROM cartoes WHERE apagado <> 'S' AND titular = 'S' AND codcartimp = '" + cartao + "')";
            }
            else
               if (nome != "")//procura pelo Nome
               {
                  sql += " and (titular like '" + (nome.Length > 3 ? "%" : "") + nome.ToUpper() + "%')";
               }
         #endregion

         #region Status do Conveniado
         switch (status)
         {
            case "L": sql += " and conveniados.liberado = 'S' "; break;
            case "B": sql += " and conveniados.liberado <> 'S' "; break;
         }
         #endregion

         sql += " group by CONTACORRENTE.DATA_FECHA_EMP, CONVENIADOS.TITULAR, CONVENIADOS.CHAPA, CONTACORRENTE.DATA_VENC_EMP, CONVENIADOS.CONV_ID, CONVENIADOS.GRUPO_CONV_EMP, CONTACORRENTE.RECEITA ";
         sql += "order by CONVENIADOS.TITULAR";

         BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }

      public static DataTable GetAutorizacoes(int empres_id, string datasFecha, bool baixados, string tipoext,
                                              string nfentreg)
      {
         string sql = " select distinct CC.DATA, CC.AUTORIZACAO_ID, CC.DIGITO, CC.DATA_FECHA_EMP, CC.CONV_ID, CAR.CODIGO as CARCOD, CAR.DIGITO as CARDIG, CAR.CODCARTIMP, ";
         sql += " coalesce(CRED.FANTASIA,CRED.NOME) as EMPRESA,";
         sql += " coalesce(sum(CC.DEBITO - CC.CREDITO),0) as VALOR, ";
         sql += " CC.HISTORICO";
         sql += " from CONVENIADOS CONV, CONTACORRENTE CC, CREDENCIADOS CRED, CARTOES CAR";
         sql += " where (CONV.EMPRES_ID = " + empres_id + ")";
         sql += " and (CONV.APAGADO <> 'S')";
         sql += " and (CC.DATA_FECHA_EMP in (" + datasFecha.Substring(0, datasFecha.Length - 1) + "))";
         sql += " and (CC.CONV_ID = CONV.CONV_ID) and (CRED.CRED_ID = CC.CRED_ID) and (CC.CARTAO_ID = CAR.CARTAO_ID)";
         if ((!baixados) || (tipoext == "P"))
            sql += " and (COALESCE(CC.BAIXA_CONVENIADO,'N') <> 'S')";
         switch (nfentreg)
         {
            case "S": sql += " and (COALESCE(CC.ENTREG_NF,'N') = 'S') "; break;
            case "N": sql += " and (COALESCE(CC.ENTREG_NF,'N') = 'N') "; break;
         }
         sql += " group by CC.DATA_FECHA_EMP, CC.CONV_ID, CC.DATA, CC.AUTORIZACAO_ID, CC.DIGITO, CRED.FANTASIA, CRED.NOME, CC.DEBITO, CC.CREDITO, CC.HISTORICO, CAR.CODIGO, CAR.DIGITO, CAR.CODCARTIMP";

         BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }

      public static DataTable GetProdutos(int empres_id, string datasFecha, bool baixados, string tipoext,
                                          string nfentreg)
      {
         string sql = " select MOV.QTDE, PROD.PROD_ID, PROD.DESCRICAO, MOV.PRECO_UNI, MOV.AUTORIZACAO_ID, ";
         sql += " coalesce(sum(MOV.PRECO_UNI * MOV.QTDE),0) as VALOR";
         sql += " from MOV_PROD2 MOV, PRODUTOS PROD, CONTACORRENTE CC, CONVENIADOS CONV";
         sql += " where (MOV.PROD_ID = PROD.PROD_ID)";
         sql += " and (MOV.AUTORIZACAO_ID = CC.AUTORIZACAO_ID)";
         sql += " and (CONV.EMPRES_ID = " + empres_id + ")";
         sql += " and (CC.CONV_ID = CONV.CONV_ID)";
         sql += " and (CONV.APAGADO <> 'S')";
         sql += " and (MOV.CANCELADO <> 'S')";
         sql += " and (CC.DATA_FECHA_EMP in (" + datasFecha.Substring(0, datasFecha.Length - 1) + "))";
         if ((!baixados) || (tipoext == "P"))
            sql += " and (COALESCE(CC.BAIXA_CONVENIADO,'N') <> 'S')";
         switch (nfentreg)
         {
            case "S": sql += " and (COALESCE(CC.ENTREG_NF,'N') = 'S') "; break;
            case "N": sql += " and (COALESCE(CC.ENTREG_NF,'N') = 'N') "; break;
         }
         sql += " group by PROD.PROD_ID, PROD.DESCRICAO, MOV.PRECO_UNI, MOV.AUTORIZACAO_ID, MOV.QTDE";

         BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }

      public static DataTable GetTransacoes(int empres_id, string dataini, string datafim, string status)
      {
         string sql = " SELECT trans.trans_id, trans.datahora, trans.confirmada, trans.cancelado,";
         sql += " conv.chapa, conv.conv_id, conv.titular, cart.codigo, cart.digito, cart.codcartimp,";
         sql += " COALESCE(pt.codbarras,'') AS codbarras, COALESCE(pt.descricao,'PRODUTO NAO ENVIADO') AS descricao,";
         sql += " COALESCE(pt.qtd_aprov,0) AS qtd_aprov,";
         sql += " COALESCE(pt.vlr_bru,trans.valor,0.00) AS vlr_bru,";
         sql += " (COALESCE(pt.vlr_desc,0.00) + COALESCE(pt.vale_utilizado,0.00)) AS vlr_desc,";
         sql += " COALESCE(pt.vlr_liq,trans.valor,0.00) AS vlr_liq,";
         sql += " CASE WHEN COALESCE(pt.status,4) = 0 THEN COALESCE(prog.nome,'GRUPO '||gp.descricao)";
         sql += " WHEN COALESCE(pt.status,4) = 1 THEN 'SEM DESCONTO'";
         sql += " WHEN COALESCE(pt.status,4) = 2 THEN 'PRODUTO BLOQUEADO'";
         sql += " WHEN COALESCE(pt.status,4) = 3 THEN 'APLICACAO '||trans.operador";
         sql += " ELSE 'PRODUTO NAO ENVIADO' END tipo_desconto";
         sql += " FROM Transacoes trans";
         sql += " LEFT JOIN Prod_Trans pt ON pt.trans_id = trans.trans_id";
         sql += " LEFT JOIN Programas prog ON pt.prog_id = prog.prog_id";
         sql += " LEFT JOIN Grupo_Prod gp ON pt.grupo_prod_id = gp.grupo_prod_id";
         sql += " JOIN Cartoes cart ON trans.cartao_id = cart.cartao_id";
         sql += " JOIN Conveniados conv ON cart.conv_id = conv.conv_id";
         sql += " WHERE conv.empres_id =" + empres_id;
         sql += " AND (trans.datahora BETWEEN '" + Convert.ToDateTime(dataini).ToString("MM/dd/yyyy 00:00:00") + "' AND '" + Convert.ToDateTime(datafim).ToString("MM/dd/yyyy 23:59:59") + "')";
         sql += " AND trans.aberta = 'N'";
         switch (status)
         {
            case "S": sql += " AND trans.confirmada = 'S' AND trans.cancelado = 'N'"; break;
            case "N": sql += " AND trans.confirmada = 'N' AND trans.cancelado = 'N'"; break;
            case "C": sql += " AND trans.cancelado = 'S'"; break;
         }
         sql += " ORDER BY conv.titular, trans.trans_id, pt.descricao";

         BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }      
      #endregion

      #region WebUsuarios
      public static DataTable GetCartoes(int conv_id, DateTime periodo, bool baixados)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("conv_id", conv_id));

         string sql = " select CARTOES.NOME, CARTOES.CODIGO as COD, CARTOES.DIGITO as DIG, " +
             " coalesce(sum(CONTACORRENTE.DEBITO - CONTACORRENTE.CREDITO),0) as VALOR " +
             " from CONTACORRENTE JOIN CARTOES ON CARTOES.CARTAO_ID = CONTACORRENTE.CARTAO_ID " +
             " where CONTACORRENTE.CONV_ID = @conv_id " +
             " and (CONTACORRENTE.CARTAO_ID = CARTOES.CARTAO_ID)  " +
             " and (CONTACORRENTE.DATA_FECHA_EMP = '" +
             periodo.ToString("MM/dd/yyyy") + "')";

         if (!baixados)
            sql += " and (COALESCE(CONTACORRENTE.BAIXA_CONVENIADO,'N') <> 'S') ";

         sql += " GROUP BY CARTOES.CODIGO, CARTOES.DIGITO, CARTOES.NOME";

         BD BD = new BD();
         return BD.GetDataTable(sql, ps);
      }

      public static DataTable GetAutorizacoes(int conv_id, DateTime periodo, bool baixados)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("conv_id", conv_id));

         string sql = " select CONTACORRENTE.DATA, CONTACORRENTE.AUTORIZACAO_ID, CONTACORRENTE.DIGITO, " +
                      " CARTOES.CODIGO as COD, CARTOES.DIGITO as DIG, " +
                      " CREDENCIADOS.FANTASIA AS CREDENCIADO," +
                      " COALESCE(SUM(CONTACORRENTE.DEBITO - CONTACORRENTE.CREDITO),0) as VALOR, " +
                      " CONTACORRENTE.HISTORICO " +
                      " from CONTACORRENTE JOIN CREDENCIADOS ON CREDENCIADOS.CRED_ID = CONTACORRENTE.CRED_ID " +
                      " JOIN CARTOES ON CARTOES.CARTAO_ID = CONTACORRENTE.CARTAO_ID " +
                      " where (CONTACORRENTE.CARTAO_ID = CARTOES.CARTAO_ID)  " +
                      " and CONTACORRENTE.CONV_ID = @conv_id" +
                      " and (CONTACORRENTE.DATA_FECHA_EMP = '" +
                      periodo.ToString("MM/dd/yyyy") + "')";

         if (!baixados)
            sql += " and (COALESCE(CONTACORRENTE.BAIXA_CONVENIADO,'N') <> 'S') ";

         sql += "GROUP BY CARTOES.CODIGO, CARTOES.DIGITO, CONTACORRENTE.DATA, CONTACORRENTE.AUTORIZACAO_ID," +
                " CONTACORRENTE.DIGITO, CREDENCIADOS.FANTASIA, CONTACORRENTE.HISTORICO";

         BD BD = new BD();
         return BD.GetDataTable(sql, ps);
      }

      public static DataTable GetProdutos(string autorizacoes)
      {
         string sql = " select SUM(MOV.QTDE) as QTDE, PROD.PROD_ID, PROD.DESCRICAO, MOV.PRECO_UNI, MOV.AUTORIZACAO_ID, ";
         sql += " coalesce(sum(MOV.PRECO_UNI * MOV.QTDE),0) as VALOR";
         sql += " from MOV_PROD2 MOV, PRODUTOS PROD, CONTACORRENTE CC";
         sql += " where (MOV.PROD_ID = PROD.PROD_ID)";
         sql += " and (MOV.CANCELADO <> 'S')";
         sql += " and (MOV.AUTORIZACAO_ID = CC.AUTORIZACAO_ID)";
         sql += " and (CC.AUTORIZACAO_ID in (" + autorizacoes.Substring(0, autorizacoes.Length - 1) + "))";
         sql += " group by PROD.PROD_ID, PROD.DESCRICAO, MOV.PRECO_UNI, MOV.AUTORIZACAO_ID, MOV.QTDE";

         BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }

      public static DataTable GetTransacoes(int conv_id, string dataini, string datafim)
      {
         string sql = " SELECT trans.trans_id, trans.datahora, trans.confirmada, trans.cancelado,";
         sql += " conv.chapa, conv.conv_id, conv.titular, cart.codigo, cart.digito, cart.codcartimp,";
         sql += " COALESCE(pt.codbarras,'') AS codbarras, COALESCE(pt.descricao,'PRODUTO NAO ENVIADO') AS descricao,";
         sql += " COALESCE(pt.qtd_aprov,0) AS qtd_aprov,";
         sql += " COALESCE(pt.vlr_bru,trans.valor,0.00) AS vlr_bru,";
         sql += " COALESCE(pt.vlr_desc,0.00) AS vlr_desc,";
         sql += " COALESCE(pt.vlr_liq,trans.valor,0.00) AS vlr_liq,";
         sql += " CASE WHEN COALESCE(pt.status,4) = 0 THEN COALESCE(prog.nome,'GRUPO '||gp.descricao)";
         sql += " WHEN COALESCE(pt.status,4) = 1 THEN 'SEM DESCONTO'";
         sql += " WHEN COALESCE(pt.status,4) = 2 THEN 'PRODUTO BLOQUEADO'";
         sql += " WHEN COALESCE(pt.status,4) = 3 THEN 'APLICACAO '||trans.operador";
         sql += " ELSE 'PRODUTO NAO ENVIADO' END tipo_desconto";
         sql += " FROM Transacoes trans";
         sql += " LEFT JOIN Prod_Trans pt ON pt.trans_id = trans.trans_id";
         sql += " LEFT JOIN Programas prog ON pt.prog_id = prog.prog_id";
         sql += " LEFT JOIN Grupo_Prod gp ON pt.grupo_prod_id = gp.grupo_prod_id";
         sql += " JOIN Cartoes cart ON trans.cartao_id = cart.cartao_id";
         sql += " JOIN Conveniados conv ON cart.conv_id = conv.conv_id";
         sql += " WHERE conv.conv_id =" + conv_id;
         sql += " AND (trans.datahora BETWEEN '" + Convert.ToDateTime(dataini).ToString("MM/dd/yyyy 00:00:00") + "' AND '" + Convert.ToDateTime(datafim).ToString("MM/dd/yyyy 23:59:59") + "')";
         sql += " AND trans.aberta = 'N'";
         sql += " AND trans.confirmada = 'S' AND trans.cancelado = 'N'";
         sql += " ORDER BY conv.titular, trans.trans_id, pt.descricao";

         BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }
      #endregion

      #region WebEstabelecimentos
      public static DataTable GetRecebidos(Extratos ext, int cred_id)
      {

         string sql = " SELECT pc.pagamento_cred_id, (pc.per_comissao + pc.per_comissao_r) AS per_comiss_pag,";
         sql += " (pc.valor_comissao + pc.valor_comissao_r) AS valor_comissao_pag,";
         sql += " pc.valor_total AS valor_bru_pag, pc.valor_pago AS valor_liq_pag, pcd.id AS empres_id,";
         sql += " COALESCE((pcd.per_comissao + pcd.per_comissao_r),pc.per_comissao, 0) AS per_comiss_emp,";
         sql += " (COALESCE((pcd.per_comissao + pcd.per_comissao_r),pc.per_comissao, 0)*pcd.valor/100) AS valor_comissao_emp,";
         sql += " CASE WHEN (COALESCE(pcd.fatura_id,0) > 0) AND (fat.tipo = 'C') THEN conv.titular ELSE emp.nome END AS nome,";
         sql += " pcd.valor AS valor_empresa, COALESCE(pcd.fatura_id,0) AS fatura_id, pc.paga_cred_por_id, conv.titular, conv.chapa, cc.autorizacao_id,";
         sql += " cc.digito, (cc.debito - cc.credito) as valor_aut, cc.data";
         sql += " FROM Pagamento_cred pc";
         sql += " JOIN Pagamento_cred_det pcd ON pc.pagamento_cred_id = pcd.pagamento_cred_id";
         sql += " JOIN Fatura fat ON fat.fatura_id = pcd.fatura_id";
         sql += " JOIN Empresas emp ON emp.empres_id = pcd.id";
         sql += " JOIN ContaCorrente cc ON cc.pagamento_cred_id = pc.pagamento_cred_id";
         sql += " JOIN Conveniados conv ON cc.conv_id = conv.conv_id";
         sql += " WHERE pc.apagado <> 'S'";
         sql += " AND pc.cred_id =" + cred_id;

         if (!ext.TodasEmpresas)
         {
            string emps = String.Empty;
            foreach (string e in ext.EmpresasMarcadas.Keys)
               emps += "," + e;
            emps = emps.Substring(1);//ignorar a primeira virgula.

            sql += " AND (pc.pagamento_cred_id IN (" + emps + ")) ";
         }
         else
         {
            sql += " AND data_pgto BETWEEN '" + ext.DataIni.ToString("MM/dd/yyyy 00:00:00") + "'";
            sql += " AND '" + ext.DataFim.ToString("MM/dd/yyyy 23:59:59") + "'";
         }

         BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }

      public static DataTable GetValeDescontoPendente(int cred_id)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("cred_id", cred_id));
         ps.Add(new Fields("data_atual", DateTime.Now));

         string sql = @"SELECT
                            x.trans_id, x.datahora, x.titular, x.credito, SUM(COALESCE(hb.debito,0)) AS debito, (x.credito - SUM(COALESCE(hb.debito,0))) AS vale_acum_saldo, x.dataexpira
                        FROM
                        (
                        SELECT
                            t.trans_id, t.datahora, c.titular, h.credito, h.dataexpira
                            FROM transacoes t
                            JOIN vale_historico h ON t.trans_id = h.trans_id AND h.cancelado = 'N'
                            JOIN conveniados c ON h.conv_id = c.conv_id AND c.apagado <> 'S'
                            WHERE t.cred_id = @cred_id
                            AND h.dataexpira >= @data_atual
                            AND h.credito > 0.00
                        ) x
                        LEFT JOIN vale_historico hb ON x.trans_id = hb.trans_id_baixa AND hb.cancelado <> 'S'
                        GROUP BY x.trans_id, x.datahora, x.credito, x.titular, x.dataexpira
                        HAVING (x.credito - SUM(COALESCE(hb.debito,0))) > 0
                        ORDER BY x.trans_id";

         BD BD = new BD();
         return BD.GetDataTable(sql, ps);
      }

      public static DataTable GetTransacoesEst(int cred_id, DateTime dataini, DateTime datafim)
      {
         string sql = " SELECT trans.trans_id, trans.datahora, ";
         sql += " conv.conv_id, conv.titular, cart.codigo, cart.digito, cart.codcartimp,";
         sql += " COALESCE(pt.codbarras,'') AS codbarras, COALESCE(pt.descricao,'PRODUTO NAO ENVIADO') AS descricao,";
         sql += " COALESCE(pt.qtd_aprov,0) AS qtd_aprov,";
         sql += " COALESCE(pt.vlr_bru,trans.valor,0.00) AS vlr_bru,";
         sql += " (COALESCE(pt.vlr_desc,0.00) + COALESCE(pt.vale_utilizado,0.00)) AS vlr_desc,";
         sql += " COALESCE(pt.vlr_liq,trans.valor,0.00) AS vlr_liq,";
         sql += " CASE WHEN COALESCE(pt.status,4) = 0 THEN COALESCE(prog.nome,'GRUPO '||gp.descricao)";
         sql += " WHEN COALESCE(pt.status,4) = 1 THEN 'SEM DESCONTO'";
         sql += " WHEN COALESCE(pt.status,4) = 2 THEN 'PRODUTO BLOQUEADO'";
         sql += " WHEN COALESCE(pt.status,4) = 3 THEN 'APLICACAO '||trans.operador";
         sql += " ELSE 'PRODUTO NAO ENVIADO' END tipo_desconto";
         sql += " FROM Transacoes trans";
         sql += " LEFT JOIN Prod_Trans pt ON pt.trans_id = trans.trans_id";
         sql += " LEFT JOIN Programas prog ON pt.prog_id = prog.prog_id";
         sql += " LEFT JOIN Grupo_Prod gp ON pt.grupo_prod_id = gp.grupo_prod_id";
         sql += " JOIN Cartoes cart ON trans.cartao_id = cart.cartao_id";
         sql += " JOIN Conveniados conv ON cart.conv_id = conv.conv_id";
         sql += " WHERE trans.cred_id =" + cred_id;
         sql += " AND (trans.datahora BETWEEN '" + dataini.ToString("MM/dd/yyyy 00:00:00") + "' AND '" + datafim.ToString("MM/dd/yyyy 23:59:59") + "')";
         sql += " AND trans.confirmada = 'S' AND trans.cancelado = 'N'";
         sql += " ORDER BY trans.trans_id";

         BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }

      public static List<Vale_Historico> GetCreditosInterlojas(int cred_id, DateTime dataini, DateTime datafim)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("cred_id", cred_id));
         ps.Add(new Fields("dataini", dataini.ToString("dd/MM/yyyy") + " 00:00:00"));
         ps.Add(new Fields("datafim", datafim.ToString("dd/MM/yyyy") + " 23:59:59"));

         string sql = @"SELECT t.trans_id, t.datahora, c.titular, h.debito
                        FROM Vale_Historico h
                        JOIN Transacoes t ON t.trans_id = h.trans_id 
                           AND t.cred_id = @cred_id 
                           AND t.datahora BETWEEN @dataini AND @datafim
                        JOIN Transacoes tb ON tb.trans_id = h.trans_id_baixa 
                           AND tb.cred_id <> @cred_id
                        JOIN Conveniados c ON h.conv_id = c.conv_id                        
                        WHERE h.cancelado <> 'S'                        
                        AND h.debito > 0
                        ORDER BY t.datahora";

         List<Vale_Historico> lista = new List<Vale_Historico>();

         BD BD = new BD();
         DataTable dt = BD.GetDataTable(sql, ps);         
         foreach (DataRow row in dt.Rows)
         {
            Vale_Historico v = new Vale_Historico();
            v.Transacao.Trans_id = Convert.ToInt32(row["trans_id"]);
            v.Transacao.Datahora = Convert.ToDateTime(row["datahora"]);                        
            v.Transacao.Cartao.Conveniado.Titular = row["titular"].ToString();
            v.Debito = float.Parse(row["debito"].ToString());
            lista.Add(v);
         }

         return lista;
      }

      public static List<Vale_Historico> GetDebitosInterlojas(int cred_id, DateTime dataini, DateTime datafim)
      {
         SqlParamsList ps = new SqlParamsList();
         ps.Add(new Fields("cred_id", cred_id));
         ps.Add(new Fields("dataini", dataini.ToString("dd/MM/yyyy") + " 00:00:00"));
         ps.Add(new Fields("datafim", datafim.ToString("dd/MM/yyyy") + " 23:59:59"));

         string sql = @"SELECT t.datahora AS databaixa, h.trans_id_baixa, tb.datahora AS datatrans, c.titular, h.debito
                        FROM Vale_Historico h
                        JOIN Transacoes t ON t.trans_id = h.trans_id 
                           AND t.cred_id <> @cred_id
                           AND t.datahora BETWEEN @dataini AND @datafim
                        JOIN Transacoes tb ON tb.trans_id = h.trans_id_baixa
                           AND tb.cred_id = @cred_id
                        JOIN Conveniados c ON h.conv_id = c.conv_id
                        WHERE h.cancelado <> 'S'                        
                        AND h.debito > 0
                        ORDER BY t.datahora";

         List<Vale_Historico> lista = new List<Vale_Historico>();

         BD BD = new BD();
         DataTable dt = BD.GetDataTable(sql, ps);
         foreach (DataRow row in dt.Rows)
         {
            Vale_Historico v = new Vale_Historico();
            v.Transacao.Datahora = Convert.ToDateTime(row["databaixa"]);
            v.Trans_baixa.Trans_id = Convert.ToInt32(row["trans_id_baixa"]);
            v.Trans_baixa.Datahora = Convert.ToDateTime(row["datatrans"]);
            v.Transacao.Cartao.Conveniado.Titular = row["titular"].ToString();
            v.Debito = float.Parse(row["debito"].ToString());
            lista.Add(v);
         }

         return lista;
      }
      #endregion
   }
}
