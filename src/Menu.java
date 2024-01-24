import java.io.*;

public class Menu {
	private int option;

	public Menu() {
		super();
	}

	public int menuPral() {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		do {

			System.out.println(" \nMENU PRINCIPAL \n");
			System.out.println("1. Mostra Autors ");
			System.out.println("2. Mostra Revistes. ");
			System.out.println("3. Mostra Articles publicats ( nom revista + nom article + nom autor) ");
			System.out.println("4. Alta autor. ");
			System.out.println("5. Alta revista. ");
			System.out.println("6. Alta article.  ");
			System.out.println("7. Actualitzar títols revistes");
			System.out.println("8. Afegeix article no publicat a revista");
			System.out.println("9. Desassigna article a revista");
			System.out.println("10. Carrega autors des de fitxer");
			System.out.println("11. Sortir. ");

			System.out.println("Esculli opció: ");
			try {
				option = Integer.parseInt(br.readLine());
			} catch (NumberFormatException | IOException e) {
				System.out.println("valor no vàlid");
				e.printStackTrace();

			}

		} while (option != 1 && option != 2 && option != 3 && option != 4 && option != 5 && option != 6 && option != 7
				&& option != 8 && option != 9 && option != 10);

		return option;
	}

	public Identity autenticacio(int intents) throws IOException {
		BufferedReader br1 = new BufferedReader(new InputStreamReader(System.in));
		System.out.println("==============HEMEROTECA====================================");
		System.out.println("============================================================");
		System.out.println("Avís: tens " + (3 - intents) + " intents per loginarte");
		System.out.println("============================================================");
		System.out.println("Inserta nom del usuari: ");
		String usuari = br1.readLine();
		System.out.println("Inserta contrasenya: ");
		String pass = br1.readLine();

		Identity identity = new Identity(usuari, pass);
		return identity;

	}

}
