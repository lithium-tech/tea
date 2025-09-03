add_library(
  samovar_network_layer STATIC
  backoff.cpp
  backoff.h
  redis_client.cpp
  redis_client.h
  samovar_client.cpp
  samovar_client.h
)

target_include_directories(samovar_network_layer PRIVATE ${CMAKE_SOURCE_DIR} )
target_link_libraries(samovar_network_layer PUBLIC
  teapot_grpc_proto
  iceberg
  hiredis
  table_public
  samovar_proto
  iceberg_common_fs
)
# TODO(gmusya): it is weird that samovar_network_layer depends on filesystem
