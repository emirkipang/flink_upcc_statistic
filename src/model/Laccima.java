package model;

import util.Helper;

public class Laccima {
	private String lac;// 0
	private String cell_id;// 1
	private String site_name;// 2
	private String vendor;// 3
	private String site_id;// 4
	private String ne_id;// 5
	private String bsc_name;// 6
	private String msc_name;// 7
	private String node;// 8
	private String longitude;// 9
	private String latitude;// 10
	private String azimuth;// 11
	private String regional_network;// 12
	private String propinsi;// 13
	private String kabupaten;// 14
	private String kecamatan;// 15
	private String kelurahan;// 16
	private String regional_channel;// 17
	private String branch;// 18
	private String subbranch;// 19
	private String address;// 20
	private String sector;// 21
	private String cluster;// 22
	private String mitra_ad;// 23
	private String cgi_prepaid;// 24
	private String cgi_postpaid;// 25
	private String ocs_cluster;// 26
	private String ocs_zone;// 27
	private String type_lacci;// 28
	private String on_air_date;// 29

	private String area; // -1
	private String lacci; // -1

	// 0 lac
	// 1 cell_id
	// 17 regional_channel
	// 18 branch
	// 19 subbranch
	// 22 cluster
	// -1 area

	public Laccima() {
		this.lacci = "UNKNOWN";
		this.regional_channel = "UNKNOWN";
		this.branch = "UNKNOWN";
		this.subbranch = "UNKNOWN";
		this.cluster = "UNKNOWN";
		this.area = "UNKNOWN";
		this.propinsi = "UNKNOWN";
	}

	public Laccima(String lacci, String regional_channel, String branch,
			String subbranch, String cluster, String area, String propinsi) {
		this.lacci = lacci;
		this.regional_channel = regional_channel;
		this.branch = branch;
		this.subbranch = subbranch;
		this.cluster = cluster;
		this.area = area;
		this.propinsi = propinsi;
	}
	
	
	public String getLacci() {
		return lacci;
	}

	public void setLacci(String lacci) {
		this.lacci = lacci;
	}

	public String getLac() {
		return lac;
	}

	public void setLac(String lac) {
		this.lac = lac;
	}

	public String getCell_id() {
		return cell_id;
	}

	public void setCell_id(String cell_id) {
		this.cell_id = cell_id;
	}

	public String getSite_name() {
		return site_name;
	}

	public void setSite_name(String site_name) {
		this.site_name = site_name;
	}

	public String getVendor() {
		return vendor;
	}

	public void setVendor(String vendor) {
		this.vendor = vendor;
	}

	public String getArea() {
		return area;
	}

	public void setArea(String area) {
		this.area = area;
	}

	public String getSite_id() {
		return site_id;
	}

	public void setSite_id(String site_id) {
		this.site_id = site_id;
	}

	public String getNe_id() {
		return ne_id;
	}

	public void setNe_id(String ne_id) {
		this.ne_id = ne_id;
	}

	public String getBsc_name() {
		return bsc_name;
	}

	public void setBsc_name(String bsc_name) {
		this.bsc_name = bsc_name;
	}

	public String getMsc_name() {
		return msc_name;
	}

	public void setMsc_name(String msc_name) {
		this.msc_name = msc_name;
	}

	public String getNode() {
		return node;
	}

	public void setNode(String node) {
		this.node = node;
	}

	public String getLongitude() {
		return longitude;
	}

	public void setLongitude(String longitude) {
		this.longitude = longitude;
	}

	public String getLatitude() {
		return latitude;
	}

	public void setLatitude(String latitude) {
		this.latitude = latitude;
	}

	public String getAzimuth() {
		return azimuth;
	}

	public void setAzimuth(String azimuth) {
		this.azimuth = azimuth;
	}

	public String getRegional_network() {
		return regional_network;
	}

	public void setRegional_network(String regional_network) {
		this.regional_network = regional_network;
	}

	public String getPropinsi() {
		return propinsi;
	}

	public void setPropinsi(String propinsi) {
		this.propinsi = propinsi;
	}

	public String getKabupaten() {
		return kabupaten;
	}

	public void setKabupaten(String kabupaten) {
		this.kabupaten = kabupaten;
	}

	public String getKecamatan() {
		return kecamatan;
	}

	public void setKecamatan(String kecamatan) {
		this.kecamatan = kecamatan;
	}

	public String getKelurahan() {
		return kelurahan;
	}

	public void setKelurahan(String kelurahan) {
		this.kelurahan = kelurahan;
	}

	public String getRegional_channel() {
		return regional_channel;
	}

	public void setRegional_channel(String regional_channel) {
		this.regional_channel = regional_channel;
	}

	public String getBranch() {
		return branch;
	}

	public void setBranch(String branch) {
		this.branch = branch;
	}

	public String getSubbranch() {
		return subbranch;
	}

	public void setSubbranch(String subbranch) {
		this.subbranch = subbranch;
	}

	public String getAddress() {
		return address;
	}

	public void setAddress(String address) {
		this.address = address;
	}

	public String getSector() {
		return sector;
	}

	public void setSector(String sector) {
		this.sector = sector;
	}

	public String getCluster() {
		return cluster;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

	public String getMitra_ad() {
		return mitra_ad;
	}

	public void setMitra_ad(String mitra_ad) {
		this.mitra_ad = mitra_ad;
	}

	public String getCgi_prepaid() {
		return cgi_prepaid;
	}

	public void setCgi_prepaid(String cgi_prepaid) {
		this.cgi_prepaid = cgi_prepaid;
	}

	public String getCgi_postpaid() {
		return cgi_postpaid;
	}

	public void setCgi_postpaid(String cgi_postpaid) {
		this.cgi_postpaid = cgi_postpaid;
	}

	public String getOcs_cluster() {
		return ocs_cluster;
	}

	public void setOcs_cluster(String ocs_cluster) {
		this.ocs_cluster = ocs_cluster;
	}

	public String getOcs_zone() {
		return ocs_zone;
	}

	public void setOcs_zone(String ocs_zone) {
		this.ocs_zone = ocs_zone;
	}

	public String getType_lacci() {
		return type_lacci;
	}

	public void setType_lacci(String type_lacci) {
		this.type_lacci = type_lacci;
	}

	public String getOn_air_date() {
		return on_air_date;
	}

	public void setOn_air_date(String on_air_date) {
		this.on_air_date = on_air_date;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return getArea() + "|"
				+ getRegional_channel() + "|"
				+ getBranch() + "|"
				+ getSubbranch() + "|"
				+ getCluster();
	}

}
