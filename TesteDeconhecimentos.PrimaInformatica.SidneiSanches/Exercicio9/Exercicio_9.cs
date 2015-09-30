using System;
using System.Collections.Generic;
using System.Text;

namespace Exercicio9
{
    public class Exercicio_9
    {
        static void Main(string[] args)
        {
            for (int i = 1000; i > 100; i--)
            {
                if (isImpar(i))
                {
                    Console.Write(i + " ");
                }

            }
            Console.ReadLine();
        }

        /// <summary>
        /// Verifica se o número é ímpar se sim retorna true se não false
        /// </summary>
        /// <param name="num"></param>
        /// <returns></returns>
        public static bool isImpar(int num)
        {
            return (num % 2 == 1 ? true : false);
        }
    }
}
