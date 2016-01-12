using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace WindowsFormsApplication1
{
    public static class Utils
    {
        public static DataTable ConvertListToDataTable<T>(List<T> lista) where T : class, new()
        {
            DataTable dtRet = new DataTable("lista");
            PropertyInfo[] propriedades = typeof(T).GetProperties();

            foreach (PropertyInfo p in propriedades)
            {
                dtRet.Columns.Add(new DataColumn
                {
                    ColumnName = p.Name,
                    DataType = (p.PropertyType.Name.Contains("Nullable")
                      ? typeof(string) : p.PropertyType)
                });
            }

            PreencherDados<T>(lista, dtRet, propriedades);
            return dtRet;

        }

         //preenche os dados
        private static void PreencherDados<T>(List<T> 
          lista, DataTable dtRet, PropertyInfo[] propriedades) 
          where T : class, new()
        {
          DataRow registro;
    
          foreach (T item in lista)
          {
            registro = dtRet.NewRow();
    
            foreach (PropertyInfo p in propriedades)
            {
              registro[p.Name] = item.GetType().GetProperty
              (p.Name).GetValue(item, null);
            }
    
            dtRet.Rows.Add(registro);
          }
        }
    }
}
