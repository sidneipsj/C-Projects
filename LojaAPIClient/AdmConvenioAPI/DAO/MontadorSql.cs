using System;
using System.Collections.Generic;
using System.Text;

namespace SQLHelperv2
{            
   /// <summary>
   /// Enumerador que especifica o tipo de instrucao será realizada
   /// </summary>
   public enum MontadorType
   {
      /// <summary>
      /// Instrucao Insert Into
      /// </summary>
      Insert,
      /// <summary>
      /// Instrucao Update
      /// </summary>
      Update
   };

   /// <summary>
   /// Classe / Lista para armazenar os campos que serao utilizados para montagem da instrucao sql
   /// </summary>
   class MontadorFieldList : List<Fields> { }

   /// <summary>
   /// Classe / Lista para armazenar os parâmetros dos comandos SQL 
   /// </summary>
   public class SqlParamsList : List<Fields> { }

   /// <summary>
   /// Classe que armazena os campos e seus valores para preencher o SqlParamsList e MontadorFieldList
   /// </summary>
   public class Fields
   {
      /// <summary>
      /// Construtor da classe
      /// </summary>
      /// <param name="pNome">Nome do campo.</param>
      /// <param name="pValor">Valor do campo.</param>
      public Fields(string pNome, object pValor)
      {
         nome = pNome;
         valor = pValor;
      }

      private string nome;
      private object valor;

      /// <summary>
      /// Propriedade do atributo nome
      /// </summary>
      /// <remarks>Realiza o GET e SET</remarks>
      public string Nome
      {
         get { return nome; }
         set { nome = value; }
      }

      /// <summary>
      /// Propriedade do atributo valor
      /// </summary>
      /// <remarks>Realiza o GET e SET</remarks>
      public object Valor
      {
         get { return valor; }
         set { valor = value; }
      }

      /// <summary>
      /// Propriedade que somente retorna o tipo do atributo valor
      /// </summary>
      public Type Tipo
      {
         get { return Valor.GetType(); }
      }
   }

   /// <summary>
   /// Classe que contem os métodos para criação da instrução sql
   /// </summary>
   public class MontadorSql
   {
      #region Atributos
      private string table, wherecmd;
      private MontadorType montadorType;
      private MontadorFieldList fields;
      private SqlParamsList whereparans;
      public string valorNull { get; set; }

      #endregion

      /// <summary>
      /// Contrutor da classe
      /// </summary>
      /// <param name="pTableName">Nome da tabela que o comando será executado</param>
      /// <param name="pMontadorType">Tipo da instrucao a ser disparada contra o Banco de Dados</param>
      public MontadorSql(string pTableName, MontadorType pMontadorType)
      {
         montadorType = pMontadorType;
         if ((pTableName == null) || (pTableName.Trim() == string.Empty))
            throw new Exception("Nome da tabela obrigatório");
         table = pTableName;
         fields = new MontadorFieldList();
      }

      /// <summary>
      /// Adiciona campos no MontadorFieldList
      /// </summary>
      /// <param name="FieldName">Nome do Campo.</param>
      /// <param name="FieldValue">Valor do Campo.</param>
      public void AddField(string FieldName, object FieldValue)
      {
         Fields.Add(new Fields(FieldName, FieldValue));
      }

      /// <summary>
      /// Método que constrói a instrução
      /// </summary>
      /// <returns>Retorna uma string com a instrução montada</returns>
      public string GetSqlString()
      {
         string sql1, sql2 = string.Empty;
         if (montadorType == MontadorType.Insert)
         {
            sql1 = "insert into " + table + "(";
            sql2 = " values(";
            foreach (Fields f in Fields)
            {
               sql1 += f.Nome + ",";
               sql2 += "@" + f.Nome + ",";
            }
            sql1 = sql1.Remove(sql1.Length - 1);//Remover ultima virgula;
            sql2 = sql2.Remove(sql2.Length - 1);//Remover ultima virgula;
            sql1 += ")";
            sql2 += ")";
         }
         else
         {
            sql1 = "update " + table + " set ";
            foreach (Fields f in Fields)
            {
               sql1 += f.Nome + " = " + "@" + f.Nome + ",";
            }
            sql1 = sql1.Remove(sql1.Length - 1);//Remover ultima virgula;
         }

         sql1 += " " + sql2;
         return sql1 + " " + wherecmd;
      }

      /// <summary>
      /// Carrega uma lista de parametros
      /// </summary>
      /// <returns>Retorna a Lista carregada</returns>
      public SqlParamsList GetParams()
      {
         SqlParamsList l = new SqlParamsList();
         foreach (Fields f in Fields)
            l.Add(new Fields(f.Nome, f.Valor));

         if (whereparans != null)
            l.AddRange(whereparans);
         return l;
      }

      /// <summary>
      /// Carrega os atributos de "where" pelos paramentros passados no método
      /// </summary>
      /// <param name="pComando">Instrucao SQL.</param>
      /// <param name="ParamList">Lista de parametros SQL.</param>
      public void SetWhere(string pComando, SqlParamsList ParamList)
      {
         if (montadorType == MontadorType.Insert)
            throw new Exception("Condição nao permitida para montador tipo insert");
         whereparans = ParamList;
         wherecmd = pComando;
      }

      #region Propriedades
      /// <summary>
      /// Propriedade do atributo fields
      /// </summary>
      /// <remarks>Realiza o GET e SET</remarks>
      internal MontadorFieldList Fields
      {
         get { return fields; }
         set { fields = value; }
      }
      #endregion

   }
}
