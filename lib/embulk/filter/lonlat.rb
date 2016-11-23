Embulk::JavaPlugin.register_filter(
  "lonlat", "org.embulk.filter.lonlat.LonlatFilterPlugin",
  File.expand_path('../../../../classpath', __FILE__))
