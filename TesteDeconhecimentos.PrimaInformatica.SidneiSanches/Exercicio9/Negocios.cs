using System;
using System.Collections.Generic;
using System.Text;

namespace Exercicio9
{
    public static class Negocios
    {
        /// <summary>
        /// Verifica se o número é ímpar se sim retorna true se não false
        /// </summary>
        /// <param name="num"></param>
        /// <returns></returns>
        public static bool isImpar(int num)
        {
            return (num % 2 == 1 ? true : false) ;            
        }
    }
}
