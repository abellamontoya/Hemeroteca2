import java.io.*;
import java.sql.*;
import java.time.LocalDate;
import java.util.Date;
import java.util.Properties;
import java.util.StringTokenizer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.UUID;

public class DBAccessor {
	private String dbname;
	private String host;
	private String port;
	private String user;
	private String passwd;
	private String schema;
	Connection conn = null;

	public void init() {
		Properties prop = new Properties();
		InputStream propStream = this.getClass().getClassLoader().getResourceAsStream("db.properties");

		try {
			prop.load(propStream);
			this.host = prop.getProperty("host");
			this.port = prop.getProperty("port");
			this.dbname = prop.getProperty("dbname");
			this.schema = prop.getProperty("schema");
		} catch (IOException e) {
			String message = "ERROR: db.properties file could not be found";
			System.err.println(message);
			throw new RuntimeException(message, e);
		}
	}

	public Connection getConnection(Identity identity) {
		String url = null;
		try {
			Class.forName("org.postgresql.Driver");

			StringBuffer sbUrl = new StringBuffer();
			sbUrl.append("jdbc:postgresql:");
			if (host != null && !host.equals("")) {
				sbUrl.append("//").append(host);
				if (port != null && !port.equals("")) {
					sbUrl.append(":").append(port);
				}
			}
			sbUrl.append("/").append(dbname);
			url = sbUrl.toString();

			conn = DriverManager.getConnection(url, identity.getUser(), identity.getPassword());
			conn.setAutoCommit(false);
		} catch (ClassNotFoundException e1) {
			System.err.println("ERROR: Loading JDBC driver");
			System.err.println(e1.getMessage());
		} catch (SQLException e2) {
			System.err.println("ERROR: Not connected to DB " + url);
			System.err.println(e2.getMessage());
		}

		if (conn != null) {
			Statement statement = null;
			try {
				statement = conn.createStatement();
				statement.executeUpdate("SET search_path TO " + this.schema);
				System.out.println("OK: connected to schema " + this.schema + " of DB " + url + " user: " + user + " password:" + passwd);
				System.out.println();
			} catch (SQLException e) {
				System.err.println("ERROR: Unable to set search_path");
				System.err.println(e.getMessage());
			} finally {
				try {
					statement.close();
				} catch (SQLException e) {
					System.err.println("ERROR: Closing statement");
					System.err.println(e.getMessage());
				}
			}
		}

		return conn;
	}

	public void altaAutor() throws SQLException, IOException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		try {
			System.out.println("Enter author values:");

			System.out.print("Author ID: ");
			int idAutor = Integer.parseInt(br.readLine());

			System.out.print("Name: ");
			String nombre = br.readLine();

			System.out.print("Birth Year (yyyy): ");
			String fechaNacimiento = br.readLine();

			System.out.print("Nationality (e.g., Dutch): ");
			String nacionalidad = br.readLine();

			System.out.print("Active (Y/N): ");
			String actiu = br.readLine();

			String sql = "INSERT INTO autors (id_autor, nom, any_naixement, nacionalitat, actiu) VALUES (?, ?, ?, ?, ?)";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setInt(1, idAutor);
				pstmt.setString(2, nombre);
				pstmt.setString(3, fechaNacimiento);
				pstmt.setString(4, nacionalidad);
				pstmt.setString(5, actiu);

				pstmt.executeUpdate();
				conn.commit();
				System.out.println("Author record successfully inserted.");
			}
		} catch (SQLException e) {
			conn.rollback();
			System.err.println("Error inserting author record into the database.");
			System.err.println(e.getMessage());
		}
	}

	public void altaRevista() throws SQLException, NumberFormatException, IOException, ParseException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		try {
			System.out.println("Enter magazine values:");

			System.out.print("Magazine ID: ");
			int idRevista = Integer.parseInt(br.readLine());

			System.out.print("Magazine Title: ");
			String titulo = br.readLine();

			System.out.print("Publication Date (yyyy-MM-dd): ");
			String fechaPublicacion = br.readLine();
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			Date datePublicacion = dateFormat.parse(fechaPublicacion);

			String sql = "INSERT INTO revistes (id_revista, titol, data_publicacio) VALUES (?, ?, ?)";
			try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
				pstmt.setInt(1, idRevista);
				pstmt.setString(2, titulo);

				java.sql.Date sqlDate = new java.sql.Date(datePublicacion.getTime());
				pstmt.setDate(3, sqlDate);

				pstmt.executeUpdate();
				conn.commit();
				System.out.println("Magazine record successfully inserted.");
			}
		} catch (SQLException | ParseException e) {
			conn.rollback();
			System.err.println("Error inserting magazine record into the database.");
			System.err.println(e.getMessage());
		}
	}

	public void altaArticle() throws SQLException, NumberFormatException, IOException, ParseException {
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

		try {
			System.out.println("Enter article values:");

			System.out.print("Article ID: ");
			int idArticulo = Integer.parseInt(br.readLine());

			System.out.print("Article Title: ");
			String titulo = br.readLine();

			System.out.print("Magazine ID (can be null): ");
			String idRevistaInput = br.readLine();
			Integer idRevista = (idRevistaInput.isEmpty()) ? null : Integer.parseInt(idRevistaInput);

			System.out.print("Author ID (can be null): ");
			String idAutorInput = br.readLine();
			Integer idAutor = (idAutorInput.isEmpty()) ? null : Integer.parseInt(idAutorInput);

			System.out.print("Theme ID:");
			String idTema = br.readLine();

			System.out.print("Is publishable? (Y/N): ");
			String publicableInput = br.readLine();
			char publicableChar = publicableInput.toUpperCase().charAt(0);
			System.out.print("Theme Description: ");
			String descripcionTema = br.readLine();

			String insertTemaSql = "INSERT INTO temes (id_tema, descripcio) VALUES (?, ?)";
			try (PreparedStatement pstmtTema = conn.prepareStatement(insertTemaSql)) {
				pstmtTema.setString(1, idTema);
				pstmtTema.setString(2, descripcionTema);
				pstmtTema.executeUpdate();
			}

			String sqlArticle = "INSERT INTO articles (id_article, id_revista, id_autor, titol, data_creacio, publicable) VALUES (?, ?, ?, ?, ?, ?)";
			try (PreparedStatement pstmtArticle = conn.prepareStatement(sqlArticle)) {
				pstmtArticle.setInt(1, idArticulo);
				pstmtArticle.setObject(2, idRevista, Types.INTEGER);
				pstmtArticle.setObject(3, idAutor, Types.INTEGER);
				pstmtArticle.setString(4, titulo);
				pstmtArticle.setDate(5, java.sql.Date.valueOf(LocalDate.now()));
				pstmtArticle.setString(6, String.valueOf(publicableChar));
				pstmtArticle.executeUpdate();
				conn.commit();
				System.out.println("Article record successfully inserted.");
			}
		} catch (SQLException e) {
			conn.rollback();
			System.err.println("Error inserting article record into the database.");
			System.err.println(e.getMessage());
		}
	}

	public void afegeixArticleARevista(Connection conn) throws SQLException, IOException {
		ResultSet rs = null;
		Statement st = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
		InputStreamReader isr = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader(isr);

		try {
			String query = "SELECT id_article, titol FROM articles WHERE id_revista IS NULL";
			rs = st.executeQuery(query);

			if (!rs.next()) {
				System.out.println("No articles pending association with magazines.");
			} else {
				rs.beforeFirst();

				while (rs.next()) {
					int idArticle = rs.getInt("id_article");
					String titolArticle = rs.getString("titol");

					System.out.println("Article Title: " + titolArticle);

					System.out.println("Do you want to add this article to a magazine? (yes/no)");
					String respuesta = br.readLine();

					if (respuesta.equalsIgnoreCase("yes")) {
						System.out.print("Enter the magazine identifier: ");
						int idRevista = Integer.parseInt(br.readLine());

						rs.updateInt("id_revista", idRevista);
						rs.updateRow();
						System.out.println("Article associated with the magazine successfully.");
					}
				}

				conn.commit();
			}
		} catch (Exception e) {
			e.printStackTrace();
			conn.rollback();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (st != null) {
				st.close();
			}
			if (br != null) {
				br.close();
			}
		}
	}

	public void actualitzarTitolRevistes(Connection conn) throws SQLException, IOException {
		ResultSet rs = null;
		String resposta = null;

		Statement st = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
		InputStreamReader isr = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader(isr);

		try {
			rs = st.executeQuery("SELECT * FROM revistes");

			if (!rs.next()) {
				System.out.println("No magazines inserted. ");
			} else {
				rs.beforeFirst();

				while (rs.next()) {
					System.out.println("Title: " + rs.getString("titol"));

					System.out.println("Do you want to change the title of this magazine?");
					resposta = br.readLine();

					if (resposta.equals("yes")) {
						System.out.println("Enter the new title");
						String nouTitol = br.readLine();

						rs.updateString("titol", nouTitol);
						rs.updateRow();
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				br.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public void desassignaArticleARevista(Connection conn) throws SQLException, IOException {
		Statement st = null;
		ResultSet rs = null;
		InputStreamReader isr = new InputStreamReader(System.in);
		BufferedReader br = new BufferedReader(isr);

		try {
			st = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);

			System.out.println("Enter the magazine identifier:");
			int revistaId = Integer.parseInt(br.readLine());

			String query = "SELECT * FROM articles WHERE id_revista =" + revistaId;
			rs = st.executeQuery(query);

			if (!rs.next()) {
				System.out.println("No hi ha articles associats a aquesta revista.");
			} else {
				System.out.println("Articles associats a la revista amb identificador " + revistaId + ":");
				do {
					String articleTitle = rs.getString("titol");
					int associatedRevistaId = rs.getInt("id_revista");
					System.out.println("Article: " + articleTitle + "\t Revista: " + associatedRevistaId);

					System.out.println("Vols rescindir la incorporació d'aquest article a la revista? (si/no)");
					String resposta = br.readLine();

					if (resposta.equalsIgnoreCase("si")) {
						rs.updateNull("id_revista");

						rs.updateRow();
						System.out.println("Incorporació rescindida amb èxit.");
					} else {
						System.out.println("Operació cancel·lada.");
					}
				} while (rs.next());
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (st != null) {
				st.close();
			}
			if (br != null) {
				br.close();
			}
		}
	}


	// TODO
	public void mostraAutors(Connection conn) throws SQLException, IOException {
		Statement st = null;
		ResultSet rs = null;

		try {
			st = conn.createStatement();

			// TODO: Perform a query to retrieve all fields of the authors
			String query = "SELECT * FROM autors";
			rs = st.executeQuery(query);

			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				System.out.print(metaData.getColumnName(i) + "\t");
			}
			System.out.println();

			while (rs.next()) {
				for (int i = 1; i <= columnCount; i++) {
					System.out.print(rs.getString(i) + "\t");
				}
				System.out.println();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (st != null) {
				st.close();
			}
		}
	}


	// TODO
	public void mostraRevistes(Connection conn) throws SQLException, IOException {
		Statement st = null;
		ResultSet rs = null;

		try {
			st = conn.createStatement();

			// TODO: Perform a query to retrieve all fields of the magazines
			String query = "SELECT * FROM revistes";
			rs = st.executeQuery(query);

			ResultSetMetaData metaData = rs.getMetaData();
			int columnCount = metaData.getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				System.out.print(metaData.getColumnName(i) + "\t");
			}
			System.out.println();

			while (rs.next()) {
				for (int i = 1; i <= columnCount; i++) {
					System.out.print(rs.getString(i) + "\t");
				}
				System.out.println();
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (st != null) {
				st.close();
			}
		}
	}


	// TODO
	public void mostraRevistesArticlesAutors(Connection conn) throws SQLException, IOException {
		Statement st = null;
		ResultSet rs = null;

		try {
			st = conn.createStatement();

			String query = "SELECT revistes.titol AS revista_titol, articles.titol AS article_titol, autors.nom AS autor_nom, temes.descripcio AS tema_descripcio " +
					"FROM articles " +
					"JOIN revistes ON articles.id_revista = revistes.id_revista " +
					"JOIN autors ON articles.id_autor = autors.id_autor " +
					"LEFT JOIN tracta ON articles.id_article = tracta.id_article " +
					"LEFT JOIN temes ON tracta.id_tema = temes.id_tema";

			rs = st.executeQuery(query);

			System.out.println("Revista\t\tArticle\t\tAutor\t\tTema");
			System.out.println("--------------------------------------------");

			while (rs.next()) {
				String revistaTitol = rs.getString("revista_titol");
				String articleTitol = rs.getString("article_titol");
				String autorNom = rs.getString("autor_nom");
				String temaDescripcio = rs.getString("tema_descripcio");

				System.out.println(revistaTitol + "\t\t" + articleTitol + "\t\t" + autorNom + "\t\t" + temaDescripcio);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (rs != null) {
				rs.close();
			}
			if (st != null) {
				st.close();
			}
		}
	}




	public void sortir() throws SQLException {
		System.out.println("ADÉU!");
		conn.close();
	}

	// TODO
	public void carregaAutors() throws SQLException, NumberFormatException, IOException {
		String csvFilePath = "C:\\Users\\abell\\IdeaProjects\\Hemeroteca\\autors.csv"; // Cambia la ruta al archivo CSV

		try (BufferedWriter bw = new BufferedWriter(new FileWriter(csvFilePath))) {
			String selectQuery = "SELECT nom, any_naixement, nacionalitat, actiu FROM autors";
			try (PreparedStatement pstmt = conn.prepareStatement(selectQuery)) {
				ResultSet rs = pstmt.executeQuery();

				bw.write("nom,any_naixement,nacionalitat,actiu");
				bw.newLine();

				while (rs.next()) {
					String nom = rs.getString("nom");
					String anyNaixement = rs.getString("any_naixement");
					String nacionalitat = rs.getString("nacionalitat");
					String actiu = rs.getString("actiu");

					String line = String.format("%s,%s,%s,%s", nom, anyNaixement, nacionalitat, actiu);
					bw.write(line);
					bw.newLine();
				}

				System.out.println("Exportación de datos de autores a CSV completada con éxito.");
			}
		} catch (IOException | SQLException e) {
			e.printStackTrace();
		}
	}
}
