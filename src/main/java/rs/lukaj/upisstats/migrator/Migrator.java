package rs.lukaj.upisstats.migrator;

import rs.lukaj.upisstats.scraper.download.DownloadController;
import rs.lukaj.upisstats.scraper.obrada2017.*;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.util.*;

import static rs.lukaj.upisstats.migrator.Main.log;

public class Migrator implements AutoCloseable, Closeable {

    private Connection conn;
    private StatementPool pool;
    private int dbYear;

    public Migrator(String user, String pass, String dbName, int dbYear) throws SQLException {
        conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/" + dbName, user, pass);
        conn.setAutoCommit(false);
        pool = new StatementPool(conn);
        this.dbYear = dbYear;
        zeljeSql = "INSERT INTO " + TableNames.LISTA_ZELJA.getName(dbYear) + "(" +
                "smer_id, ucenik_id, ispunio_uslov, bodova_za_upis, krug, redni_broj) VALUES (?, ?, ?, ?, ?, ?)";
        prijemniSql = "INSERT INTO " + TableNames.PRIJEMNI.getName(dbYear) + "(" +
                "ucenik_id, naziv_ispita, bodova) VALUES (?, ?, ?)";
    }

    public boolean tablesExist() throws SQLException {
        DatabaseMetaData md = conn.getMetaData();
        ResultSet rs = md.getTables(null, null, "%", null);
        Set<String> wantedTables = new HashSet<>();
        for (TableNames name : TableNames.values())
            if (name.tableExists(dbYear))
                wantedTables.add(name.getName(dbYear));
        while (rs.next()) {
            String tableName = rs.getString(3);
            if (wantedTables.contains(tableName)) return true;
        }
        return false;
    }

    public void loadToDatabase(String dataYear) throws SQLException {
        if (tablesExist() && !Main.OVERWRITE_DATA) throw new RuntimeException("Data is already there!");

        DownloadController.DATA_FOLDER = new File("/home/luka/Documents/upis/data/" + dataYear);
        UceniciBase.load();

        log("Loaded data from files; starting migration.");
        loadSmerovi();
        log("Loaded smerovi.");
        loadOsnovne();
        log("Loaded osnovne.");
        loadUcenici();
        log("Loaded ucenici.");
        populateAverages();
        log("Calculated averages. Cleaning up.");

        UceniciBase.clear();
        log("Done!");
    }

    private void loadSmerovi() throws SQLException {
        Collection<SmerW> smerovi = SmeroviBase.getAll();
        PreparedStatement stmt = pool.get("INSERT INTO " + TableNames.SMEROVI.getName(dbYear) + " (" +
                "sifra, ime, mesto, okrug, smer, podrucje, kvota, jezik, trajanje, kvota_umanjenje, " +
                "upisano_1k, upisano_2k, kvota_2k, min_bodova_1k, min_bodova_2k, broj_ucenika) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)");
        for (SmerW smer : smerovi) {
            stmt.setString(1, smer.sifra);
            stmt.setString(2, smer.skola);
            stmt.setString(3, smer.opstina);
            stmt.setString(4, smer.okrug);
            stmt.setString(5, smer.smer);
            stmt.setString(6, smer.podrucje);
            stmt.setInt(7, smer.kvota);
            stmt.setString(8, smer.jezik);
            stmt.setInt(9, smer.trajanje);
            stmt.setInt(10, smer.kvotaUmanjenje);
            stmt.setInt(11, smer.upisano1k);
            stmt.setInt(12, smer.upisano2k);
            stmt.setInt(13, smer.kvota2k);
            stmt.setDouble(14, smer.minBodova1k);
            stmt.setDouble(15, smer.minBodova2k);
            stmt.addBatch();
        }
        stmt.executeBatch();
        stmt.close();
        conn.commit();
    }

    private void loadOsnovne() throws SQLException {
        Collection<OsnovnaW> osnovne = OsnovneBase.getAll();
        PreparedStatement stmt = pool.get("INSERT INTO " + TableNames.OSNOVNE.getName(dbYear) + " (" +
                "ime, mesto, opstina, okrug, ukupno_ucenika, svrsenih_ucenika, nesvrsenih_ucenika, vukovaca, " +
                "nagradjenih, svi_bodova6, svi_bodova7, svi_bodova8, svi_prosek6, svi_prosek7, svi_prosek8," +
                "svi_bodova_ukupno, svi_bodova_zavrsni, svi_bodova_ocene, svi_prosek_ukupno, id, broj_ucenika) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)");
        for (OsnovnaW os : osnovne) {
            stmt.setString(1, os.naziv);
            stmt.setString(2, os.opstina);
            stmt.setString(3, os.opstina);
            stmt.setString(4, os.okrug);
            stmt.setInt(5, os.brojUcenika);
            stmt.setInt(6, os.ucenikaZavrsilo);
            stmt.setInt(7, os.nijeZavrsilo);
            stmt.setInt(8, os.vukovaca);
            stmt.setInt(9, os.nagradjenih);
            stmt.setDouble(10, os.bodova6);
            stmt.setDouble(11, os.bodova7);
            stmt.setDouble(12, os.bodova8);
            stmt.setDouble(13, os.prosek6);
            stmt.setDouble(14, os.prosek7);
            stmt.setDouble(15, os.prosek8);
            stmt.setDouble(16, os.ukupnoBodova);
            stmt.setDouble(17, os.bodovaZavrsni);
            stmt.setDouble(18, os.bodovaOcene);
            stmt.setDouble(19, os.prosecnaOcena);
            stmt.setLong(20, os.id);
            stmt.addBatch();
        }
        stmt.executeBatch();
        stmt.close();
        conn.commit();
    }

    private long getSmerId(String sifra) throws SQLException {
        PreparedStatement stmt = pool.get("SELECT id FROM " + TableNames.SMEROVI.getName(dbYear) +
                " WHERE sifra=?");
        stmt.setString(1, sifra);
        try (stmt; ResultSet res = stmt.executeQuery()) {
            if (!res.next()) return -1;
            return res.getLong(1);
        }
    }

    private void incrementBrojUcenika(String table, long id) throws SQLException {
        PreparedStatement stmt = pool.get("UPDATE " + table + " SET broj_ucenika=broj_ucenika+1 " +
                "WHERE id=?");
        stmt.setLong(1, id);
        stmt.executeUpdate();
    }

    private int loadingCount = 0;

    private void loadUcenici() throws SQLException {
        PreparedStatement stmt = pool.get("INSERT INTO " + TableNames.UCENICI.getName(dbYear) + " (" +
                "sifra, drugi_strani_jezik, prosek_sesti, prosek_sedmi, prosek_osmi, prosek_ukupno, " +
                "matematika, srpski, kombinovani, bodovi_iz_skole, bodovi_sa_zavrsnog, bodovi_ukupno, " +
                "bodovi_sa_prijemnog, bodovi_sa_takmicenja, broj_zelja, upisana_zelja, krug, najbolji_blizanac_bodovi, " +
                "blizanac_sifra, bodova_am, maternji_jezik, prvi_strani_jezik, vukova_diploma, prioritet, osnovna_id," +
                "upisana_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        PreparedStatement idStmt = pool.get("SELECT currval('" + TableNames.UCENICI.getSequenceName(dbYear) + "')");
        try {
            UceniciBase.svi().forEach((ThrowingSqlConsumer<UcenikW>) uc -> {
                if (++loadingCount % 5000 == 0) log("Loaded " + loadingCount + " ucenika.");
                if (loadingCount % 10000 == 0) Profiler.printTimes();

                long start = System.nanoTime();
                stmt.setInt(1, uc.sifra);
                stmt.setString(2, uc.drugiStrani);
                stmt.setDouble(3, uc.sestiRaz.prosekOcena);
                stmt.setDouble(4, uc.sedmiRaz.prosekOcena);
                stmt.setDouble(5, uc.osmiRaz.prosekOcena);
                stmt.setDouble(6, uc.prosekUkupno);
                stmt.setDouble(7, uc.matematika);
                stmt.setDouble(8, uc.srpski);
                stmt.setDouble(9, uc.kombinovani);
                stmt.setDouble(10, uc.bodoviOcene);
                stmt.setDouble(11, uc.bodovaZavrsni);
                stmt.setDouble(12, uc.ukupnoBodova);
                stmt.setDouble(13, 0); //todo this properly (if possible)
                stmt.setDouble(14, uc.bodovaTakmicenja);
                stmt.setInt(15, uc.krug == 1 ? uc.listaZelja1.size() : uc.krug == 2 ? uc.listaZelja2.size() : 0);
                stmt.setInt(16, uc.upisanaZelja);
                stmt.setInt(17, uc.krug);
                stmt.setDouble(18, uc.najboljiBlizanacBodovi);
                stmt.setInt(19, uc.blizanacSifra);
                stmt.setDouble(20, uc.bodovaAM);
                stmt.setString(21, uc.maternji);
                stmt.setString(22, uc.prviStrani);
                stmt.setBoolean(23, uc.vukovaDiploma);
                stmt.setBoolean(24, uc.prioritet);
                stmt.setLong(25, uc.osnovna.id);
                long smerId = getSmerId(uc.smer.sifra);
                stmt.setLong(26, smerId);
                stmt.executeUpdate();
                long endUcInsert = System.nanoTime();
                incrementBrojUcenika(TableNames.OSNOVNE.getName(dbYear), uc.osnovna.id);
                incrementBrojUcenika(TableNames.SMEROVI.getName(dbYear), smerId);
                long endIncrement = System.nanoTime();
                ResultSet resId = idStmt.executeQuery();
                if (!resId.next()) throw new SQLException("Got no values when fetching currval for ucenici!");
                long id = resId.getLong(1);
                long endGetId = System.nanoTime();
                loadOcene(uc, id);
                long ocene = System.nanoTime();
                loadListaZelja(uc, id);
                long listaZelja = System.nanoTime();
                loadPrijemni(uc, id);
                long prijemni = System.nanoTime();
                loadTakmicenja(uc, id);
                long takmicenja = System.nanoTime();
                Profiler.addTime("insertUcenik", endUcInsert - start);
                Profiler.addTime("incrementCounts", endIncrement - endUcInsert);
                Profiler.addTime("getUcenikId", endGetId - endIncrement);
                Profiler.addTime("insertOcene", ocene - endGetId);
                Profiler.addTime("insertListaZelja", listaZelja - ocene);
                Profiler.addTime("insertPrijemni", prijemni - listaZelja);
                Profiler.addTime("insertTakmicenja", takmicenja - prijemni);
            });
            long otherBatches = System.nanoTime();
            int count = pool.executeBatchesPrefix("UPDATE " + TableNames.UCENICI.getName(dbYear) + " SET ");
            long prijemniBatch = System.nanoTime();
            pool.executeBatch(prijemniSql);
            long zeljeBatch = System.nanoTime();
            pool.executeBatch(zeljeSql);
            long endBatches = System.nanoTime();
            log("Executed " + (count + 2) + " batches. Finishing.");
            conn.commit();
            long commit = System.nanoTime();
            Profiler.addTime("uceniciBatches", prijemniBatch-otherBatches);
            Profiler.addTime("prijemniBatch", zeljeBatch - prijemniBatch);
            Profiler.addTime("zeljeBatch", endBatches - zeljeBatch);
            Profiler.addTime("commitUcenici", commit - endBatches);
            Profiler.printTimes();
        } catch (UncheckedSQLException e) {
            throw (SQLException) e.getCause();
        } finally {
            stmt.close();
            idStmt.close();
        }
    }

    private String zeljeSql;
    private void loadListaZelja(UcenikW uc, long id) throws SQLException {
        PreparedStatement stmt = pool.get(zeljeSql);
        loadZelje(stmt, id, uc.listaZelja1, (short) 1);
        loadZelje(stmt, id, uc.listaZelja2, (short) 2);
    }

    private void loadZelje(PreparedStatement stmt, long ucenikId, List<UcenikW.Zelja> listaZelja, short krug)
            throws SQLException {
        for (UcenikW.Zelja z : listaZelja) {
            stmt.setLong(1, getSmerId(z.smer.sifra));
            stmt.setLong(2, ucenikId);
            stmt.setBoolean(3, z.uslov);
            stmt.setDouble(4, z.bodovaZaUpis);
            stmt.setShort(5, krug);
            stmt.setInt(6, z.redniBroj);
            stmt.addBatch();
        }
    }

    private String prijemniSql;
    private void loadPrijemni(UcenikW uc, long ucenikId) throws SQLException {
        if(uc.prijemni.isEmpty()) return;
        PreparedStatement stmt = pool.get(prijemniSql);
        for(Map.Entry<String, Double> p : uc.prijemni.entrySet()) {
            stmt.setLong(1, ucenikId);
            stmt.setString(2, p.getKey());
            stmt.setDouble(3, p.getValue());
            stmt.addBatch();
        }
    }

    private void loadTakmicenja(UcenikW uc, long id) throws SQLException {
        UcenikW.Takmicenje tak = uc.takmicenje;
        if(tak == null) return;
        PreparedStatement stmt = pool.get("INSERT INTO " + TableNames.TAKMICENJA.getName(dbYear) + "(" +
                "ucenik_id, predmet, bodova, mesto, rang) VALUES (?, ?, ?, ?, ?)");
        stmt.setLong(1, id);
        stmt.setString(2, tak.predmet);
        stmt.setDouble(3, tak.bodova);
        stmt.setInt(4, tak.mesto);
        stmt.setInt(5, tak.nivo);
        stmt.executeUpdate();
    }

    private void loadOcene(UcenikW uc, long id) throws SQLException {
        loadOceneRazred(uc.sestiRaz, 6, id);
        loadOceneRazred(uc.sedmiRaz, 7, id);
        loadOceneRazred(uc.osmiRaz, 8, id);
        populateUcenikAverages(uc, id);
    }

    private void loadOceneRazred(UcenikW.Ocene ocene, int razred, long id) throws SQLException {
        String table = TableNames.UCENICI.getName(dbYear);
        Set<Map.Entry<String, Integer>> entries = ocene.ocene.entrySet();
        if(entries.isEmpty()) return;
        StringBuilder sql = new StringBuilder("UPDATE " + table + " SET ");
        for(Map.Entry<String, Integer> ocena : entries) {
            String colName = getDbName(ocena.getKey()) + razred;
            sql.append(colName).append("=?,");
        }
        sql.deleteCharAt(sql.length()-1);
        sql.append(" WHERE id=?");
        PreparedStatement stmt = pool.get(sql.toString());
        int i=1;
        for(Map.Entry<String, Integer> ocena : entries) {
            stmt.setInt(i, ocena.getValue());
            i++;
        }
        stmt.setLong(i, id);
        stmt.addBatch();
    }

    private void populateUcenikAverages(UcenikW uc, long id) throws SQLException {
        String table = TableNames.UCENICI.getName(dbYear);
        Set<String> predmeti = new HashSet<>();
        uc.sestiRaz.ocene.keySet().forEach((p) -> predmeti.add(getDbName(p)));
        uc.sedmiRaz.ocene.keySet().forEach((p) -> predmeti.add(getDbName(p)));
        uc.osmiRaz.ocene.keySet().forEach((p) -> predmeti.add(getDbName(p)));
        if(predmeti.isEmpty()) return;
        StringBuilder sql = new StringBuilder("UPDATE " + table + " SET ");
        for(String p : predmeti) {
            String colName = getDbName(p) + "_p";
            sql.append(colName).append("=?,");
        }
        sql.deleteCharAt(sql.length()-1);
        sql.append(" WHERE id=?");
        PreparedStatement stmt = pool.get(sql.toString());
        int i=1;
        for(String p : predmeti) {
            int r6 = uc.sestiRaz.ocene.getOrDefault(p, 0);
            int r7 = uc.sedmiRaz.ocene.getOrDefault(p, 0);
            int r8 = uc.osmiRaz.ocene.getOrDefault(p, 0);
            int nonzero = 3 - Utils.count(0, r6, r7, r8);
            double avg;
            if(nonzero > 0) avg = (double)(r6+r7+r8)/nonzero;
            else            avg = 0;
            stmt.setDouble(i, avg);
            i++;
        }
        stmt.setLong(i, id);
        stmt.addBatch();
    }

    private void populateAverages() throws SQLException {
        PreparedStatement schema = conn.prepareStatement("SELECT column_name, data_type FROM information_schema.columns " +
                "WHERE table_schema='public' AND table_name=?");
        schema.setString(1, TableNames.OSNOVNE.getName(dbYear));
        Map<String, String> osSchema = Utils.extractSchema(schema.executeQuery());
        schema.setString(1, TableNames.SMEROVI.getName(dbYear));
        Map<String, String> smSchema = Utils.extractSchema(schema.executeQuery());
        schema.setString(1, TableNames.UCENICI.getName(dbYear));
        ResultSet ucSchema = schema.executeQuery();
        while(ucSchema.next()) {
            String type = ucSchema.getString("data_type");
            String col = ucSchema.getString("column_name");
            if(!(type.equals("integer") || type.equals("double precision"))) continue;
            if(osSchema.containsKey(col) && osSchema.get(col).equals("double precision")) {
                calcAverage(col, "osnovna_id", TableNames.OSNOVNE);
            }
            if(smSchema.containsKey(col) && smSchema.get(col).equals("double precision")) {
                calcAverage(col, "upisana_id", TableNames.SMEROVI);
            }
        }
        schema.close();
        conn.commit();
    }

    private void calcAverage(String col, String groupingCol, TableNames table) throws SQLException {
        String ucTable = TableNames.UCENICI.getName(dbYear);
        String grTable = table.getName(dbYear);
        log("Calc avg for " + col + " @ " + groupingCol);
        PreparedStatement stmt = conn.prepareStatement("update " + grTable + " set " + col +
                "=(select avg(" + col + ") from " + ucTable +
                " where " + groupingCol + "=" + grTable + ".id)");
        stmt.executeUpdate();
        stmt.close();
    }

    @Override
    public void close() throws IOException {
        try {
            pool.close();
            conn.close();
        } catch (SQLException e) {
            throw new IOException(e);
        }
    }

    private static String getDbName(String predmet) {
        switch (predmet) {
            case "izbornisport":
            case "izborniSport":
                return "sport";
            case "prviStrani":
                return "engleski"; //backwards compatibility
            case "maternjiJezik":
                return "srpski";   //backwards compatibility
            case "drugiMaternjiJezik":
                return "drugi_maternji";
            default:
                return Utils.toSnakeCase(predmet);
        }
    }
}
