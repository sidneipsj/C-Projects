using System;
using System.Collections.Generic;
using System.Text;

namespace Exercicio10
{
    public class Program
    {
        static void Main(string[] args)
        {
            int cont = 1;
            int maior = 0, menor = 1000;
            List<int> lista100Numeros = new List<int>();
            Console.WriteLine("Digite 100 numeros aleatórios...");
            Console.WriteLine();
            while (cont <= 5)
            {   
                Console.WriteLine("Digite o " + cont + " numero");
                lista100Numeros.Add(Convert.ToInt16(Console.ReadLine()));
                cont++;
            }

            foreach (var item in lista100Numeros)
            {
                if (item > maior)
                    maior = item;
                if (item < menor)
                    menor = item;
            }
            Console.WriteLine("O maior número é: " + maior +"\n" + "O menor número é: "+menor);
            Console.ReadKey();

        }
    }
}
