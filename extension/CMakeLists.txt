file(READ tea_ext_create.sql TEA_EXT_CREATE_SCRIPT)
file(READ tea_ext_update.sql TEA_EXT_UPDATE_SCRIPT)
if(TEA_BUILD_FDW)
  file(READ tea_fdw_create.sql TEA_FDW_CREATE_SCRIPT)
  file(READ tea_fdw_update.sql TEA_FDW_UPDATE_SCRIPT)
endif()
if(TEA_BUILD_STATS)
  file(READ tea_stats_create.sql TEA_STATS_CREATE_SCRIPT)
  file(READ tea_stats_update.sql TEA_STATS_UPDATE_SCRIPT)
endif()

set(EXT_FILES)
macro(install_template TEMPLATE_FILE DEST_FILE)
  set(DEST_PATH ${CMAKE_CURRENT_BINARY_DIR}/${DEST_FILE})
  configure_file(${TEMPLATE_FILE} ${DEST_PATH} @ONLY)
  list(APPEND EXT_FILES ${DEST_PATH})
endmacro()

install_template(tea.control.in tea.control)
install_template(tea_secondary.control.in tea--${TEA_VERSION}.control)

install_template(tea_install.sql.in tea--${TEA_VERSION}.sql)
foreach(TEA_BASE_VERSION ${TEA_BASE_VERSIONS})
  install_template(tea_upgrade.sql.in tea--${TEA_BASE_VERSION}--${TEA_VERSION}.sql)
  install_template(tea_upgrade.sql.in tea--${TEA_VERSION}--${TEA_BASE_VERSION}.sql)
endforeach()

install(
  FILES ${EXT_FILES}
  DESTINATION share/postgresql/extension
  COMPONENT tea)
