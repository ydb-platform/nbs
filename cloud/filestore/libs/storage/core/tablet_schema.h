#pragma once

#include "public.h"

#include <ydb/core/tablet_flat/flat_cxx_database.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <
    ui32 channel = 1,
    NKikimr::NTable::NPage::ECache cache = NKikimr::NTable::NPage::ECache::None,
    NKikimr::NTable::NPage::ECodec codec = NKikimr::NTable::NPage::ECodec::Plain
>
struct TStoragePolicy
{
    static constexpr ui32 Room = 0;
    static constexpr ui32 Channel = channel;

    static constexpr ui32 Family = 0;
    static constexpr NKikimr::NTable::NPage::ECache Cache = cache;
    static constexpr NKikimr::NTable::NPage::ECodec Codec = codec;
};

////////////////////////////////////////////////////////////////////////////////

enum class ECompactionPolicy
{
    None,
    Default,
    IndexTable,
};

template <ECompactionPolicy compactionPolicy = ECompactionPolicy::None>
struct TCompactionPolicy
{
    static constexpr ECompactionPolicy CompactionPolicy = compactionPolicy;
};

void InitCompactionPolicy(
    NKikimr::NTable::TAlter& alter,
    ui32 tableId,
    ECompactionPolicy compactionPolicy);

////////////////////////////////////////////////////////////////////////////////

template <NKikimr::NIceDb::TTableId TableId>
struct TTableSchema
    : public NKikimr::NIceDb::Schema::Table<TableId>
{
    using StoragePolicy = TStoragePolicy<>;
    using CompactionPolicy = TCompactionPolicy<>;

    template <NKikimr::NIceDb::TColumnId ColumnId, typename MessageType>
    struct ProtoColumn : NKikimr::NIceDb::Schema::Table<TableId>::template
        Column<ColumnId, NKikimr::NScheme::NTypeIds::String>
    {
        using Type = MessageType;
    };
};

////////////////////////////////////////////////////////////////////////////////

template <typename Type>
struct TSchemaInitializer;

template <typename Type>
struct TSchemaInitializer<NKikimr::NIceDb::Schema::SchemaTables<Type>>
{
    static void
    InitStorage(bool useNoneCompactionPolicy, NKikimr::NTable::TAlter& alter)
    {
        alter.SetRoom(
            Type::TableId,
            Type::StoragePolicy::Room,
            Type::StoragePolicy::Channel,
            {Type::StoragePolicy::Channel},
            Type::StoragePolicy::Channel);

        alter.SetFamily(
            Type::TableId,
            Type::StoragePolicy::Family,
            Type::StoragePolicy::Cache,
            Type::StoragePolicy::Codec);

        // See #688. By default, all filestore tablets are created with
        // compaction policy set to None. UseNoneCompactionPolicy flag
        // allows to continue using None policy instead of the one set
        // in the schema.
        // TODO(debnatkh): remove this workaround after completion of #688.
        InitCompactionPolicy(
            alter,
            Type::TableId,
            useNoneCompactionPolicy
                ? ECompactionPolicy::None
                : Type::CompactionPolicy::CompactionPolicy);
    }
};

template <typename Type, typename... Types>
struct TSchemaInitializer<NKikimr::NIceDb::Schema::SchemaTables<Type, Types...>>
{
    static void InitStorage(bool useNoneCompactionPolicy, NKikimr::NTable::TAlter& alter)
    {
        TSchemaInitializer<NKikimr::NIceDb::Schema::SchemaTables<Type>>::
            InitStorage(useNoneCompactionPolicy, alter);
        TSchemaInitializer<NKikimr::NIceDb::Schema::SchemaTables<Types...>>::
            InitStorage(useNoneCompactionPolicy, alter);
    }
};

}   // namespace NCloud::NFileStore::NStorage
