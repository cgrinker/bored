#pragma once

#include "bored/ddl/ddl_command.hpp"
#include "bored/ddl/ddl_dispatcher.hpp"

namespace bored::ddl {

void register_catalog_handlers(DdlCommandDispatcher& dispatcher);

}  // namespace bored::ddl
