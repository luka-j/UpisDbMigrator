package rs.lukaj.upisstats.migrator;

public enum TableNames {
    UCENICI("ucenici", 2015),
    SMEROVI("smerovi", 2015),
    OSNOVNE("os", 2015),
    TAKMICENJA("takmicenja", 2015),
    PRIJEMNI("prijemni", 2017),
    LISTA_ZELJA("lista_zelja", 2015);

    public final String name;
    public final int availableSince;

    TableNames(String name, int availableSince) {
        this.name = name;
        this.availableSince = availableSince;
    }

    public boolean tableExists(int year) {
        return year >= availableSince;
    }

    public String getName(int year) {
        if(!tableExists(year))
            throw new UnsupportedOperationException("Table " + name + " exists since " + availableSince + "; didn't exist in " + year);
        return name + year;
    }

    public String getSequenceName(int year) {
        if(!tableExists(year))
            throw new UnsupportedOperationException("Table " + name + " exists since " + availableSince + "; didn't exist in " + year);
        return getName(year) + "_id_seq";
    }
}
