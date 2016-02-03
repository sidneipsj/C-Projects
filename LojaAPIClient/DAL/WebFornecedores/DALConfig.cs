using System;
using System.Collections.Generic;
using System.Text;
using Negocio;
using System.Data;
using SQLHelperv2;
using Misc;

namespace DAL
{   
    [Serializable()]
    public struct ConfiguracoesWebFornecedores
    {
        public string MOSTRAR_TOTAL_EXTRATO;
        public string TOTAL_EXTRATO_TODAS_PAG;
        public string PAGINAR_GRADE_EMP;
        public string MOSTRAR_TOTAL_EMP;
        public string MOSTRAR_TOTAL_EXTRATO_EMP;
        public string TOTAL_EXTRATO_EMP_TODAS_PAG;
        public string PAGINAR_EXTRATO;
        public bool USA_ENTREG_NF;
        public string EXIBIR_PAGTO_SITE;
    }

    public class DALConfig
    {

        public ConfiguracoesWebFornecedores GetConfiguracoesWebFornecedores(int cred_id)
        {
           BD BD = new BD();
            SafeDataReader dr = BD.GetDataReader("SELECT * FROM Config_webfornecedores WHERE cred_id = " + cred_id, null);
            ConfiguracoesWebFornecedores conf = new ConfiguracoesWebFornecedores();

            try
            {
                if (dr.Read())
                {
                    conf.MOSTRAR_TOTAL_EMP = dr.GetString("MOSTRAR_TOTAL_EMP");
                    conf.MOSTRAR_TOTAL_EXTRATO = dr.GetString("MOSTRAR_TOTAL_EXTRATO");
                    conf.MOSTRAR_TOTAL_EXTRATO_EMP = dr.GetString("MOSTRAR_TOTAL_EXTRATO_EMP");
                    conf.PAGINAR_GRADE_EMP = dr.GetString("PAGINAR_GRADE_EMP");
                    conf.TOTAL_EXTRATO_EMP_TODAS_PAG = dr.GetString("TOTAL_EXTRATO_EMP_TODAS_PAG");
                    conf.TOTAL_EXTRATO_TODAS_PAG = dr.GetString("TOTAL_EXTRATO_TODAS_PAG");
                    conf.PAGINAR_EXTRATO = dr.GetString("PAGINAR_EXTRATO");
                }
                else
                {
                    conf.MOSTRAR_TOTAL_EMP = "S";
                    conf.MOSTRAR_TOTAL_EXTRATO = "S";
                    conf.MOSTRAR_TOTAL_EXTRATO_EMP = "S";
                    conf.PAGINAR_GRADE_EMP = "S";
                    conf.TOTAL_EXTRATO_EMP_TODAS_PAG = "S";
                    conf.TOTAL_EXTRATO_TODAS_PAG = "S";
                    conf.PAGINAR_EXTRATO = "S";
                }
            }
            finally
            {
                dr.Close();
            }
            dr = BD.GetDataReader("SELECT * FROM Config", null);

            try
            {
                if (dr.Read())
                    conf.EXIBIR_PAGTO_SITE = dr.GetString("EXIBIR_PAGTO_SITE");
                else
                    conf.EXIBIR_PAGTO_SITE = "S";
            }
            finally
            {
                dr.Close();
            }

            conf.USA_ENTREG_NF = WebConfig.UsarNfEntrega;
            return conf;
        }
    }  
}    
