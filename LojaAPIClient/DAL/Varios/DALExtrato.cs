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

       public static DataTable GetFechamentosAlimentacaoRefeicaoConv(int empres_id, string chapa,string cartao, string nome, string status, string setor)
       {
           chapa = chapa.Trim().Equals("") ? "null" : chapa;
           cartao = cartao.Trim().Equals("") ? "null" : "'" + cartao + "'";
           nome = nome.Trim().Equals("") ? "null" : "'" + nome + "'";
           status = status.Trim().Equals("") || status.Trim().Equals("T") ? "null" : "'" + status + "'";
           setor = setor.Trim().Equals("") || setor.Trim().Equals("0") ? "null" : "'" + setor + "'";

           BD BD = new BD();
           string sql;

           sql = " SELECT CONV.CONV_ID, CONV.TITULAR, CART.CODCARTIMP, CONCAT(EMP.EMPRES_ID, ' - ', EMP.NOME) AS NOME, AH.DATA_RENOVACAO, AH.RENOVACAO_VALOR, AH.ABONO_VALOR,";
           sql += " AH.SALDO_ACUMULADO_ANT, COALESCE(SUM(CC.DEBITO - CC.CREDITO),0.00) CONSUMO,"; 
           sql += " (AH.RENOVACAO_VALOR + AH.ABONO_VALOR + AH.SALDO_ACUMULADO_ANT) - COALESCE(SUM(CC.DEBITO - CC.CREDITO),0.00) AS SALDO_ACUMULADO_PROX,";
           sql += " COALESCE(AH.DATA_LANCAMENTO, AH.DATA_RENOVACAO) AS DATA_LANCAMENTO, AH.DIAS_TRABALHADOS";
           sql += " FROM CONVENIADOS CONV"; 
           sql += " INNER JOIN CARTOES CART ON CART.CONV_ID = CONV.CONV_ID";  
           sql += " INNER JOIN EMPRESAS EMP ON EMP.EMPRES_ID = CONV.EMPRES_ID";  
           sql += " INNER JOIN ALIMENTACAO_HISTORICO AH ON AH.CONV_ID = CONV.CONV_ID"; 
           sql += " LEFT JOIN CONTACORRENTE CC ON CC.CONV_ID = CONV.CONV_ID";  
           sql += " AND CC.DATA BETWEEN AH.DATA_RENOVACAO AND COALESCE((SELECT TOP 1 DATEADD(DD,-1,DATA_RENOVACAO) FROM ALIMENTACAO_HISTORICO WHERE";  
           sql += " CONV_ID = CONV.CONV_ID";  
           sql += " AND DATA_RENOVACAO > AH.DATA_RENOVACAO";   
           sql += " ORDER BY DATA_RENOVACAO ASC), CONVERT(DATE,CURRENT_TIMESTAMP))"; 
           sql += " WHERE CART.APAGADO <> 'S' "; 
           sql += " AND coalesce(CART.titular,'S') = 'S'"; 
           sql += " and emp.empres_id = coalesce(" + empres_id + ",conv.empres_id) ";
           sql += " and conv.chapa = coalesce(" + chapa + ",conv.chapa) ";
           sql += " and lower(conv.titular) like '%' + lower(coalesce(" + nome + ",conv.titular)) + '%' ";
           sql += " and conv.liberado = coalesce(" + status + ",conv.liberado) ";
           sql += " and coalesce(conv.setor,'') = coalesce(" + setor + ",conv.setor,'') ";
           sql += " and CART.codcartimp = coalesce(" + cartao + ",CART.codcartimp) ";
           sql += " GROUP BY CONV.TITULAR, CART.CODCARTIMP, EMP.NOME, AH.DATA_RENOVACAO, AH.RENOVACAO_VALOR, AH.ABONO_VALOR,AH.SALDO_ACUMULADO_ANT,";
           sql += " CONV.CONV_ID, AH.SALDO_ACUMULADO_PROX,";
           sql += " AH.DATA_LANCAMENTO, AH.DIAS_TRABALHADOS, EMP.EMPRES_ID ";
           sql += " ORDER BY EMP.NOME, CONV.TITULAR, AH.DATA_RENOVACAO";

           return BD.GetDataTable(sql, null);
           
       }

       public static DataTable GetCreditosAlimentacaoRefeicaoConv(int empres_id, string chapa, string cartao, string nome, string status, string setor)
       {
           chapa = chapa.Trim().Equals("") ? "null" : chapa;
           cartao = cartao.Trim().Equals("") ? "null" : "'" + cartao + "'";
           nome = nome.Trim().Equals("") ? "null" : "'" + nome + "'";
           setor = setor.Trim().Equals("") || setor.Trim().Equals("0") ? "null" : "'" + setor + "'";

           BD BD = new BD();
           string sql;

           sql = "  select  conv.CONV_ID, cart.CODCARTIMP, conv.TITULAR, coalesce(alc.RENOVACAO_VALOR,0.00) as RENOVACAO_VALOR, Coalesce(alc.ABONO_VALOR,0.00) as ABONO_VALOR,";
           sql += " CONVERT(VARCHAR, alr.DATA_RENOVACAO,103) AS DATA_RENOVACAO,COALESCE(CONVERT(VARCHAR,alc.DATA_ALTERACAO,120),CONVERT(VARCHAR,alr.DATA_RENOVACAO,120)) AS DATA_ALTERACAO, emp.NOME ";   
           sql += " from CONVENIADOS conv";
           sql += " inner join cartoes cart on cart.CONV_ID = conv.conv_id";
           sql += " inner join empresas emp on emp.EMPRES_ID = conv.EMPRES_ID"; 
           sql += " inner join ALIMENTACAO_RENOVACAO alr on alr.EMPRES_ID = conv.EMPRES_ID"; 
           sql += " left join ALIMENTACAO_RENOVACAO_CREDITOS alc on alc.RENOVACAO_ID = alr.RENOVACAO_ID and conv.CONV_ID = alc.CONV_ID";
           sql += " WHERE CART.APAGADO <> 'S' ";
           sql += " AND coalesce(CART.titular,'S') = 'S'";
           sql += " and emp.empres_id = coalesce(" + empres_id + ",conv.empres_id) ";
           sql += " and conv.chapa = coalesce(" + chapa + ",conv.chapa) ";
           sql += " and lower(conv.titular) like '%' + lower(coalesce(" + nome + ",conv.titular)) + '%' ";
           sql += " and conv.liberado = coalesce('" + status + "',conv.liberado) ";
           sql += " and coalesce(conv.setor,'') = coalesce(" + setor + ",conv.setor,'') ";
           sql += " and CART.codcartimp = coalesce(" + cartao + ",CART.codcartimp) ";
           sql += " ORDER BY CONV.TITULAR";

           return BD.GetDataTable(sql, null);

       }



      public static DataTable GetFechamentosAlimentacaoRefeicao(string dataini, string datafin, int empres_id, string chapa,
                                                                string cartao, string nome, string status, string setor) 
      {
          SqlParamsList sl = new SqlParamsList();
          if (dataini == null || dataini.Trim().Equals(""))
          {
              sl.Add(new Fields("dataini", null));
          }
          else
          {
              sl.Add(new Fields("dataini", Convert.ToDateTime(dataini)));
          }
          if (datafin == null || datafin.Trim().Equals(""))
          {
              sl.Add(new Fields("datafin", null));
          }
          sl.Add(new Fields("empres_id", empres_id));
          if (chapa.Trim().Equals(""))
          {
              sl.Add(new Fields("chapa", null));
          }
          else
          {
              sl.Add(new Fields("chapa", Convert.ToInt64(chapa)));
          }
          sl.Add(new Fields("cartao", (cartao.Trim().Equals("") ? null : cartao)));
          sl.Add(new Fields("titular", (nome.Trim().Equals("") ? null : nome)));
          sl.Add(new Fields("status", (status.Trim().Equals("") || status.Trim().Equals("T") ? null : status)));
          sl.Add(new Fields("setor", (setor.Trim().Equals("") || setor.Trim().Equals("0") ? null : setor)));
         
          BD BD = new BD();
          return BD.GetDataTable(" titular, empresa_desc, conv_id_ret, cartao_ret, data, credito, acumulado_mes_ant, gasto, acumulado_prox_mes from extrato_alim_ref(@dataini,@datafin,@empres_id,@chapa,@cartao,@titular,@status,@setor)", sl);
      }

      //Alterado para SqlServer
      public static DataTable GetFechamentos(bool vzerado, bool baixados, string tipoext, string nfentreg,
                                             string datafecha, string dataini, string datafim, int empres_id,
                                             string chapa, string cartao, string nome, string status, string grupo, string setor)
      {
         int band_id = DALConveniado.GetEmpresBandId(empres_id);
         int qtdLimites = DALConveniado.GetQtdLimites(empres_id);
         string sql = " select CONV.CONV_ID, CONV.CHAPA, CONV.TITULAR, CONV.GRUPO_CONV_EMP, ";
         sql += " CONTACORRENTE.DATA_FECHA_EMP, ";
         sql += " CONTACORRENTE.DATA_VENC_EMP, ";
         sql += " coalesce(sum(CONTACORRENTE.DEBITO - CONTACORRENTE.CREDITO),0) as VALOR "; //<-- ISSO AQUI ERA OQ TAVA ANTES... FOI ALTERADO PRA PODER ADICIONAR OS OUTROS VALORES DOS SALDOS (01/12/2012)
         if (band_id != 999)
         {
           if (qtdLimites >= 1)
           {
               sql += ", (select coalesce(sum (debito - credito),0)" +
                      " from contacorrente cc" +
                      " join credenciados cred on cred.cred_id = cc.cred_id" +
                      " where conv_id = conv.conv_id" +
                      " and receita = 'S'" +
                      " and data_fecha_emp = CONTACORRENTE.DATA_FECHA_EMP" +
                      " and coalesce(cc.entreg_nf,'N') = 'S'" +
                      " and cred.seg_id in (select seg_id" +
                      "        from bandeiras_segmentos bs" +
                      "            where bs.band_id = " + band_id +
                      "               and bs.cod_limite = 1)" +
                      " ) as SALDO_COM_RECEITA_LIM_1," +
                      " (select coalesce(sum (debito - credito),0)" +
                      " from contacorrente cc" +
                      " join credenciados cred on cred.cred_id = cc.cred_id" +
                      " where conv_id = conv.conv_id" +
                      " and receita = 'N'" +
                      " and data_fecha_emp = CONTACORRENTE.DATA_FECHA_EMP" +
                      " and coalesce(cc.entreg_nf, 'N') = 'S' and" +
                      " cred.seg_id in (select seg_id" +
                      "              from bandeiras_segmentos bs" +
                      "             where bs.band_id = " + band_id +
                      "               and bs.cod_limite = 1)" +
                      " ) as SALDO_SEM_RECEITA_LIM_1," +
                      " (select coalesce(sum (debito - credito),0)" +
                      " from contacorrente cc" +
                      " join credenciados cred on cred.cred_id = cc.cred_id" +
                      " where conv_id = conv.conv_id" +
                      " and receita = 'S'" +
                      " and data_fecha_emp = CONTACORRENTE.DATA_FECHA_EMP" +
                      " and coalesce(cc.entreg_nf, 'N') = 'N' and" +
                      " cred.seg_id in (select seg_id" +
                      "				from bandeiras_segmentos bs" +
                      "             where bs.band_id = " + band_id +
                      "              and bs.cod_limite = 1)" +
                      " ) as SALDO_COM_RECEITA_N_CONF_LIM_1," +
                      " (select coalesce(sum (debito - credito),0)" +
                      " from contacorrente cc" +
                      " join credenciados cred on cred.cred_id = cc.cred_id" +
                      " where conv_id = conv.conv_id" +
                      " and receita = 'N'" +
                      " and data_fecha_emp = CONTACORRENTE.DATA_FECHA_EMP" +
                      " and coalesce(cc.entreg_nf, 'N') = 'N' and" +
                      " cred.seg_id in (select seg_id" +
                      "             from bandeiras_segmentos bs" +
                      "             where bs.band_id = " + band_id +
                      "               and bs.cod_limite = 1)" +
                      " ) as SALDO_SEM_RECEITA_N_CONF_LIM_1 ";
           }
           if (qtdLimites >= 2)
           {
               sql += ", (select coalesce(sum (debito - credito),0)" +
                     " from contacorrente cc" +
                     " join credenciados cred on cred.cred_id = cc.cred_id" +
                     " where conv_id = conv.conv_id" +
                     " and receita = 'S'" +
                     " and data_fecha_emp = CONTACORRENTE.DATA_FECHA_EMP" +
                     " and coalesce(cc.entreg_nf,'N') = 'S'" +
                     " and cred.seg_id in (select seg_id" +
                     "        from bandeiras_segmentos bs" +
                     "            where bs.band_id = " + band_id +
                     "               and bs.cod_limite = 2)" +
                     " ) as SALDO_COM_RECEITA_LIM_2," +
                     " (select coalesce(sum (debito - credito),0)" +
                     " from contacorrente cc" +
                     " join credenciados cred on cred.cred_id = cc.cred_id" +
                     " where conv_id = conv.conv_id" +
                     " and receita = 'N'" +
                     " and data_fecha_emp = CONTACORRENTE.DATA_FECHA_EMP" +
                     " and coalesce(cc.entreg_nf, 'N') = 'S' and" +
                     " cred.seg_id in (select seg_id" +
                     "              from bandeiras_segmentos bs" +
                     "             where bs.band_id = " + band_id +
                     "               and bs.cod_limite = 2)" +
                     " ) as SALDO_SEM_RECEITA_LIM_2," +
                     " (select coalesce(sum (debito - credito),0)" +
                     " from contacorrente cc" +
                     " join credenciados cred on cred.cred_id = cc.cred_id" +
                     " where conv_id = conv.conv_id" +
                     " and receita = 'S'" +
                     " and data_fecha_emp = CONTACORRENTE.DATA_FECHA_EMP" +
                     " and coalesce(cc.entreg_nf, 'N') = 'N' and" +
                     " cred.seg_id in (select seg_id" +
                     "				from bandeiras_segmentos bs" +
                     "             where bs.band_id = " + band_id +
                     "              and bs.cod_limite = 2)" +
                     " ) as SALDO_COM_RECEITA_N_CONF_LIM_2," +
                     " (select coalesce(sum (debito - credito),0)" +
                     " from contacorrente cc" +
                     " join credenciados cred on cred.cred_id = cc.cred_id" +
                     " where conv_id = conv.conv_id" +
                     " and receita = 'N'" +
                     " and data_fecha_emp = CONTACORRENTE.DATA_FECHA_EMP" +
                     " and coalesce(cc.entreg_nf, 'N') = 'N' and" +
                     " cred.seg_id in (select seg_id" +
                     "             from bandeiras_segmentos bs" +
                     "             where bs.band_id = " + band_id +
                     "               and bs.cod_limite = 2)" +
                     " ) as SALDO_SEM_RECEITA_N_CONF_LIM_2 ";
           }
           if (qtdLimites >= 3)
           {
               sql += ", (select coalesce(sum (debito - credito),0)" +
                     " from contacorrente cc" +
                     " join credenciados cred on cred.cred_id = cc.cred_id" +
                     " where conv_id = conv.conv_id" +
                     " and receita = 'S'" +
                     " and data_fecha_emp = CONTACORRENTE.DATA_FECHA_EMP" +
                     " and coalesce(cc.entreg_nf,'N') = 'S'" +
                     " and cred.seg_id in (select seg_id" +
                     "        from bandeiras_segmentos bs" +
                     "            where bs.band_id = " + band_id +
                     "               and bs.cod_limite = 3)" +
                     " ) as SALDO_COM_RECEITA_LIM_3," +
                     " (select coalesce(sum (debito - credito),0)" +
                     " from contacorrente cc" +
                     " join credenciados cred on cred.cred_id = cc.cred_id" +
                     " where conv_id = conv.conv_id" +
                     " and receita = 'N'" +
                     " and data_fecha_emp = CONTACORRENTE.DATA_FECHA_EMP" +
                     " and coalesce(cc.entreg_nf, 'N') = 'S' and" +
                     " cred.seg_id in (select seg_id" +
                     "              from bandeiras_segmentos bs" +
                     "             where bs.band_id = " + band_id +
                     "               and bs.cod_limite = 3)" +
                     " ) as SALDO_SEM_RECEITA_LIM_3," +
                     " (select coalesce(sum (debito - credito),0)" +
                     " from contacorrente cc" +
                     " join credenciados cred on cred.cred_id = cc.cred_id" +
                     " where conv_id = conv.conv_id" +
                     " and receita = 'S'" +
                     " and data_fecha_emp = CONTACORRENTE.DATA_FECHA_EMP" +
                     " and coalesce(cc.entreg_nf, 'N') = 'N' and" +
                     " cred.seg_id in (select seg_id" +
                     "				from bandeiras_segmentos bs" +
                     "             where bs.band_id = " + band_id +
                     "              and bs.cod_limite = 3)" +
                     " ) as SALDO_COM_RECEITA_N_CONF_LIM_3," +
                     " (select coalesce(sum (debito - credito),0)" +
                     " from contacorrente cc" +
                     " join credenciados cred on cred.cred_id = cc.cred_id" +
                     " where conv_id = conv.conv_id" +
                     " and receita = 'N'" +
                     " and data_fecha_emp = CONTACORRENTE.DATA_FECHA_EMP" +
                     " and coalesce(cc.entreg_nf, 'N') = 'N' and" +
                     " cred.seg_id in (select seg_id" +
                     "             from bandeiras_segmentos bs" +
                     "             where bs.band_id = " + band_id +
                     "               and bs.cod_limite = 3)" +
                     " ) as SALDO_SEM_RECEITA_N_CONF_LIM_3 ";
           }
           if (qtdLimites >= 4)
           {
               sql += ", (select coalesce(sum (debito - credito),0)" +
                     " from contacorrente cc" +
                     " join credenciados cred on cred.cred_id = cc.cred_id" +
                     " where conv_id = conv.conv_id" +
                     " and receita = 'S'" +
                     " and data_fecha_emp = CONTACORRENTE.DATA_FECHA_EMP" +
                     " and coalesce(cc.entreg_nf,'N') = 'S'" +
                     " and cred.seg_id in (select seg_id" +
                     "        from bandeiras_segmentos bs" +
                     "            where bs.band_id = " + band_id +
                     "               and bs.cod_limite = 4)" +
                     " ) as SALDO_COM_RECEITA_LIM_4," +
                     " (select coalesce(sum (debito - credito),0)" +
                     " from contacorrente cc" +
                     " join credenciados cred on cred.cred_id = cc.cred_id" +
                     " where conv_id = conv.conv_id" +
                     " and receita = 'N'" +
                     " and data_fecha_emp = CONTACORRENTE.DATA_FECHA_EMP" +
                     " and coalesce(cc.entreg_nf, 'N') = 'S' and" +
                     " cred.seg_id in (select seg_id" +
                     "              from bandeiras_segmentos bs" +
                     "             where bs.band_id = " + band_id +
                     "               and bs.cod_limite = 4)" +
                     " ) as SALDO_SEM_RECEITA_LIM_4," +
                     " (select coalesce(sum (debito - credito),0)" +
                     " from contacorrente cc" +
                     " join credenciados cred on cred.cred_id = cc.cred_id" +
                     " where conv_id = conv.conv_id" +
                     " and receita = 'S'" +
                     " and data_fecha_emp = CONTACORRENTE.DATA_FECHA_EMP" +
                     " and coalesce(cc.entreg_nf, 'N') = 'N' and" +
                     " cred.seg_id in (select seg_id" +
                     "				from bandeiras_segmentos bs" +
                     "             where bs.band_id = " + band_id +
                     "              and bs.cod_limite = 4)" +
                     " ) as SALDO_COM_RECEITA_N_CONF_LIM_4," +
                     " (select coalesce(sum (debito - credito),0)" +
                     " from contacorrente cc" +
                     " join credenciados cred on cred.cred_id = cc.cred_id" +
                     " where conv_id = conv.conv_id" +
                     " and receita = 'N'" +
                     " and data_fecha_emp = CONTACORRENTE.DATA_FECHA_EMP" +
                     " and coalesce(cc.entreg_nf, 'N') = 'N' and" +
                     " cred.seg_id in (select seg_id" +
                     "             from bandeiras_segmentos bs" +
                     "             where bs.band_id = " + band_id +
                     "               and bs.cod_limite = 4)" +
                     " ) as SALDO_SEM_RECEITA_N_CONF_LIM_4 ";
           }
           sql += " from CONVENIADOS CONV ";
           sql += (vzerado ? " left " : "") + "join CONTACORRENTE on ( CONV.CONV_ID = CONTACORRENTE.CONV_ID ) ";

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
             sql += " and (CONTACORRENTE.DATA_FECHA_EMP = '" + Convert.ToDateTime(datafecha).ToString("dd/MM/yyyy") + "')";
           else if (tipoext == "P") //extrato periodo
             sql += " and (CONTACORRENTE.DATA between '" + Convert.ToDateTime(dataini).ToString("dd/MM/yyyy") + "' and '" + Convert.ToDateTime(datafim).ToString("dd/MM/yyyy") + "')";
           #endregion

           sql += " join empresas emp on conv.empres_id = emp.empres_id ";
           sql += " join bandeiras b on b.band_id = emp.band_id ";
           sql += " left join bandeiras_conv bc on conv.conv_id = bc.conv_id ";
         }
         else
         {
           //sql += " coalesce(sum(CONTACORRENTE.DEBITO - CONTACORRENTE.CREDITO),0) as VALOR "; //<-- ISSO AQUI ERA OQ TAVA ANTES... FOI ALTERADO PRA PODER ADICIONAR OS OUTROS VALORES DOS SALDOS (01/12/2012)
           sql += " from CONVENIADOS CONV ";
           sql += (vzerado ? " left " : "") + "join CONTACORRENTE on ( CONV.CONV_ID = CONTACORRENTE.CONV_ID ) ";

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
             sql += " and (CONTACORRENTE.DATA_FECHA_EMP = '" + Convert.ToDateTime(datafecha).ToString("dd/MM/yyyy") + "')";
           else if (tipoext == "P") //extrato periodo
             sql += " and (CONTACORRENTE.DATA between '" + Convert.ToDateTime(dataini).ToString("dd/MM/yyyy") + "' and '" + Convert.ToDateTime(datafim).ToString("dd/MM/yyyy") + "')";
           #endregion

         }
         sql += " where (CONV.APAGADO <> 'S')";
         sql += " and (CONV.EMPRES_ID = " + empres_id + ")";
         #region Conveniado

         netUtil.Funcoes funcoes = new netUtil.Funcoes();

         if (grupo != "0")
            sql += " and conv.grupo_conv_emp=" + grupo;
         if (setor != "0")
            sql += " and conv.setor = '" + setor + "'";
         if (funcoes.SoNumero(chapa) != "")
            sql += " and conv.chapa = " + funcoes.SoNumero(chapa);
         else
            if (funcoes.SoNumero(cartao) != "") //procura pelo cartao
            {
               if ((cartao.Length == 11) && (Convert.ToInt32(cartao.Substring(9, 2)) == funcoes.DigitoCartao(Convert.ToInt32(cartao.Substring(0, 9)))))
                  sql += " and conv.conv_id = (SELECT conv_id FROM cartoes WHERE codigo = " + cartao.Substring(0, 9) + ")";
               else //cartao de importação
                  sql += " and conv.conv_id = (SELECT top 1 conv_id FROM cartoes WHERE apagado <> 'S' AND titular = 'S' AND codcartimp = '" + cartao + "')";
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
            case "L": sql += " and conv.liberado = 'S' "; break;
            case "B": sql += " and conv.liberado <> 'S' "; break;
         }
         #endregion

         //if (band_id != 999)
         //{
         //  string campos = "";
         //  if (qtdLimites >= 1)
         //     campos = ", SALDO_COM_RECEITA_LIM_1, SALDO_SEM_RECEITA_LIM_1, SALDO_COM_RECEITA_N_CONF_LIM_1, SALDO_SEM_RECEITA_N_CONF_LIM_1 ";
         //  if (qtdLimites >= 2)
         //    campos += ", SALDO_COM_RECEITA_LIM_2, SALDO_SEM_RECEITA_LIM_2, SALDO_COM_RECEITA_N_CONF_LIM_2, SALDO_SEM_RECEITA_N_CONF_LIM_2 ";
         //  if (qtdLimites >= 3)
         //    campos += ", SALDO_COM_RECEITA_LIM_3, SALDO_SEM_RECEITA_LIM_3, SALDO_COM_RECEITA_N_CONF_LIM_3, SALDO_SEM_RECEITA_N_CONF_LIM_3 ";
         //  if (qtdLimites >= 4)
         //    campos += ", SALDO_COM_RECEITA_LIM_4, SALDO_SEM_RECEITA_LIM_4, SALDO_COM_RECEITA_N_CONF_LIM_4, SALDO_SEM_RECEITA_N_CONF_LIM_4 ";
         //  sql += " group by CONTACORRENTE.DATA_FECHA_EMP, CONV.TITULAR, CONV.CHAPA, CONTACORRENTE.DATA_VENC_EMP, CONV.CONV_ID, CONV.GRUPO_CONV_EMP " + campos;
         //}
         //else
           sql += " group by CONTACORRENTE.DATA_FECHA_EMP, CONV.TITULAR, CONV.CHAPA, CONTACORRENTE.DATA_VENC_EMP, CONV.CONV_ID, CONV.GRUPO_CONV_EMP ";
         sql += "order by CONV.TITULAR";

         BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }

      public static DataTable GetAutorizacoes(int empres_id, string datasFecha, bool baixados, string tipoext,
                                              string nfentreg)
      {
        string sql = " select distinct CC.NF, CC.DATA, CC.AUTORIZACAO_ID, CC.DIGITO, CC.DATA_FECHA_EMP, CC.CONV_ID, CAR.CODIGO as CARCOD, CAR.DIGITO as CARDIG, CAR.CODCARTIMP, coalesce(CC.RECEITA,'N') RECEITA, ";
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
         sql += " group by CC.NF, CC.DATA_FECHA_EMP, CC.CONV_ID, CC.DATA, CC.AUTORIZACAO_ID, CC.DIGITO, CRED.FANTASIA, CRED.NOME, CC.DEBITO, CC.CREDITO, CC.HISTORICO, CAR.CODIGO, CAR.DIGITO, CAR.CODCARTIMP, CC.RECEITA ";

         BD BD = new BD();
         return BD.GetDataTable(sql, null);
      }

      //Alterado para SqlServer
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
      //Alterado para SqlServer
      public static Double GetCupom(int conv_id, int ano, int mes)
      {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("conv_id", conv_id));

        BD BD = new BD();
        int empresId = BD.ExecuteScalar(-1,"select top 1 empres_id from conveniados where apagado <> 'S' and conv_id = @conv_id", ps);
        
        ps.Add(new Fields("fechamento", DALEmpresa.getDataFechamento(empresId, mes, ano)) );
        ps.Add(new Fields("cod_limite", "N"));

        DataTable dt = new DataTable();
        dt = BD.GetDataTable("exec SALDO_PROXFECHA_CONV " + conv_id + ",N,'" + DALEmpresa.getDataFechamento(empresId, mes, ano)+ "'", null);
        if (dt.Rows.Count != 0)
        {
            return Convert.ToDouble(dt.Rows[0]["TOTAL"]);
        }
        else
            return 0;
        //return Convert.ToDouble(BD.ExecuteScalar("select coalesce(sum(total),0.00) total_saldo from saldo_conv(@conv_id) where  fechamento = @fechamento", ps));
      }

      //Alterado para SqlServer
      public static Double GetDescontoEmFolha(int conv_id)
      {
        SqlParamsList ps = new SqlParamsList();
        ps.Add(new Fields("conv_id", conv_id));

        BD BD = new BD();
        string sql = "SELECT (SELECT coalesce(sum(case when coalesce(cc.entreg_nf,'N')  = 'S' then cc.debito-cc.credito else 0 end),0))+ "
            + " coalesce(sum(case when coalesce(cc.entreg_nf,'N') <> 'S' then cc.debito-cc.credito else 0 end),0)as total "
            + " from DIA_FECHA "
            + " left join CONTACORRENTE cc on cc.CONV_ID = @conv_id "
            + " and cc.DATA_FECHA_EMP = DIA_FECHA.DATA_FECHA "
            + " Where DIA_FECHA.EMPRES_ID = cc.empres_id AND "
            + " DIA_FECHA.DATA_FECHA > current_timestamp ";
        return Convert.ToDouble(BD.ExecuteScalar(sql, ps));
        //return Convert.ToDouble(BD.ExecuteScalar("select sum(total) total_saldo from saldo_conv(@conv_id) where fechamento > current_timestamp",ps));
      }

      //Alterado para SqlServer
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
             periodo.ToString("dd/MM/yyyy") + "')";

         if (!baixados)
            sql += " and (COALESCE(CONTACORRENTE.BAIXA_CONVENIADO,'N') <> 'S') ";

         sql += " GROUP BY CARTOES.CODIGO, CARTOES.DIGITO, CARTOES.NOME";

         BD BD = new BD();
         return BD.GetDataTable(sql, ps);
      }
      
      //Alterado para SqlServer
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
                      periodo.ToString("dd/MM/yyyy") + "')";

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

      //Alterado para SqlServer
      public static DataTable GetTransacoes(int conv_id, string dataini, string datafim)
      {
         string sql = " SELECT trans.trans_id, trans.datahora, trans.confirmada, trans.cancelado,";
         sql += " conv.chapa, conv.conv_id, conv.titular, cart.codigo, cart.digito, cart.codcartimp,";
         sql += " COALESCE(pt.codbarras,'') AS codbarras, COALESCE(pt.descricao,'PRODUTO NAO ENVIADO') AS descricao,";
         sql += " COALESCE(pt.qtd_aprov,0) AS qtd_aprov,";
         sql += " COALESCE(pt.vlr_bru,trans.valor,0.00) AS vlr_bru,";
         sql += " COALESCE(pt.vlr_desc,0.00) AS vlr_desc,";
         sql += " COALESCE(pt.vlr_liq,trans.valor,0.00) AS vlr_liq,";
         sql += " CASE WHEN COALESCE(pt.status,4) = 0 THEN COALESCE(prog.nome,'GRUPO '+gp.descricao)";
         sql += " WHEN COALESCE(pt.status,4) = 1 THEN 'SEM DESCONTO'";
         sql += " WHEN COALESCE(pt.status,4) = 2 THEN 'PRODUTO BLOQUEADO'";
         sql += " WHEN COALESCE(pt.status,4) = 3 THEN 'APLICACAO '+trans.operador";
         sql += " ELSE 'PRODUTO NAO ENVIADO' END tipo_desconto";
         sql += " FROM Transacoes trans";
         sql += " LEFT JOIN Prod_Trans pt ON pt.trans_id = trans.trans_id";
         sql += " LEFT JOIN Programas prog ON pt.prog_id = prog.prog_id";
         sql += " LEFT JOIN Grupo_Prod gp ON pt.grupo_prod_id = gp.grupo_prod_id";
         sql += " JOIN Cartoes cart ON trans.cartao_id = cart.cartao_id";
         sql += " JOIN Conveniados conv ON cart.conv_id = conv.conv_id";
         sql += " WHERE conv.conv_id =" + conv_id;
         sql += " AND (trans.datahora BETWEEN '" + Convert.ToDateTime(dataini).ToString("dd/MM/yyyy 00:00:00") + "' AND '" + Convert.ToDateTime(datafim).ToString("dd/MM/yyyy 23:59:59") + "')";
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
