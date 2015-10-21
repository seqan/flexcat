// ==========================================================================
// Author: Benjamin Menkuec <benjamin@menkuec.de>
// ==========================================================================
#pragma once

#include <future>
#include <functional>

//#include "helper_functions.h"

namespace ptc
{
    struct NoSemaphore {};
    constexpr unsigned int defaultSleepMS = 10;

    /*
    http://www.open-std.org/jtc1/sc22/wg21/docs/papers/2014/n4058.pdf
    wait until this proposal gets accepted, then use atomic<unique_ptr<...>>
    */

    // reads read sets from hd and puts them into slots, waits if no free slots are available
    template<typename TSource, typename TItem, typename TSemaphore>
    struct Produce
    {
        using item_type = TItem;

    private:
        TSource _source;
        std::vector<std::atomic<TItem*>> _tlsItems;
        std::thread _thread;
        std::atomic_bool _eof;
        unsigned int _sleepMS;
        TSemaphore slotEmptySemaphore;
        TSemaphore readAvailableSemaphore;

    public:
        Produce(TSource& source, unsigned int sleepMS = defaultSleepMS)
            : _source(source), _eof(false), _sleepMS(sleepMS)
        {
            for (auto& item : _tlsItems)
                item.store(nullptr);  // fill initialization does not work for atomics
        }
        ~Produce()
        {
            if (_thread.joinable())
                _thread.join();
        }
        void start(const unsigned int numSlots)
        {
            /*
            - check if empty slot is available, if yes, read data into it
            - set eof=true if no more data available or max_number_of_reads reached
            - return from thread if eof == true
            - go to sleep if no free slot is available
            */
            _tlsItems = std::vector<std::atomic<TItem*>>(numSlots); // can not use resize, because TItem is not copyable if it contains unique_ptr
            _thread = std::thread([this]()
            {
                std::unique_ptr <TItem> currentItem;
                bool noEmptySlot = true;
                while (true)
                {
                    noEmptySlot = true;
                    for (auto& item : _tlsItems)
                    {
                        if (item.load(std::memory_order_relaxed) == nullptr)
                        {
                            noEmptySlot = false;
                            currentItem = std::make_unique<TItem>();
                            if (_source(currentItem))
                                item.store(currentItem.release(), std::memory_order_relaxed);
                            else
                                _eof.store(true, std::memory_order_release);

                            if (!std::is_same<TSemaphore, NoSemaphore>::value && _eof.load(std::memory_order_relaxed))  // wakeup all potentially waiting threads so that they can be joined
                                readAvailableSemaphore.signal(_tlsItems.size());
                            else if (!std::is_same<TSemaphore, NoSemaphore>::value)
                                readAvailableSemaphore.signal();
                        }
                        if (_eof.load(std::memory_order_relaxed))
                            return;
                    }
                    if (noEmptySlot)  // no empty slow was found, wait a bit
                    {
                        if (!std::is_same<TSemaphore, NoSemaphore>::value)
                            slotEmptySemaphore.wait();
                        else
                            std::this_thread::sleep_for(std::chrono::milliseconds(_sleepMS));
                    }
                }
            });
        }
        inline bool eof() const noexcept
        {
            return _eof.load(std::memory_order_acquire);
        }
        bool idle() noexcept
        {
            if (!_eof.load(std::memory_order_acquire))
                return false;
            for (auto& item : _tlsItems)
                if (item.load(std::memory_order_relaxed))
                    return false;
            return true;
        }
        /*
        do the following steps sequentially
        - check if any slot contains data, if yes take data out and return true
        - check if eof is reached, if yes return false
        - go to sleep until data is available
        */
        bool getItem(std::unique_ptr<TItem>& returnItem) noexcept
        {
            TItem* temp = nullptr;
            while (true)
            {
                bool eof = _eof.load(std::memory_order_acquire);
                for (auto& item : _tlsItems)
                {
                    if ((temp = item.load(std::memory_order_relaxed)) != nullptr)
                    {
                        if (item.compare_exchange_strong(temp, nullptr, std::memory_order_relaxed))
                        {
                            returnItem.reset(temp);
                            if (!std::is_same<TSemaphore, NoSemaphore>::value)
                                slotEmptySemaphore.signal();
                            return true;
                        }
                    }
                }
                //std::cout << std::this_thread::get_id() << "-2" << std::endl;
                if (eof) // only return if _eof == true AND all the slots are empty -> therefore read eof BEFORE checking the slots
                    return false;
                if (!std::is_same<TSemaphore, NoSemaphore>::value)
                    readAvailableSemaphore.wait();
                else
                    std::this_thread::sleep_for(std::chrono::milliseconds(_sleepMS));
            }
            return false;
        };
    };

    template <typename TProducer, typename TTransformer, typename TConsumer>
    auto make_ptc_unit(TProducer& producer, TTransformer& transformer, TConsumer& consumer, const unsigned int numThreads)
    {
        return PTC_unit<TProducer, TTransformer, TConsumer>(producer, transformer, consumer, numThreads);
    }

    template <typename TProducer, typename TTransformer, typename TConsumer>
    struct PTC_unit
    {
    private:
        // main thread variables
        TProducer& _producer;
        TTransformer& _transformer;
        TConsumer& _consumer;
        std::vector<std::thread> _threads;

    public:
        PTC_unit(TProducer& producer, TTransformer& transformer, TConsumer& consumer, const unsigned int numThreads) :
            _producer(producer), _transformer(transformer), _consumer(consumer), _threads(numThreads) {};

        void start()
        {
            _producer.start(_threads.size() + 1);
            _consumer.start(_threads.size() + 1);
            for (auto& _thread : _threads)
            {
                _thread = std::thread([this]()
                {
                    std::unique_ptr<typename TProducer::item_type> item;
                    while (_producer.getItem(item))
                    {
                        _consumer.pushItem(_transformer(std::move(item)));
                    }
                });
            }
        }
        void waitForFinish()
        {
            for (auto& _thread : _threads)
                if (_thread.joinable())
                    _thread.join();
            while (!_consumer.idle())
            {
            };
            _consumer.shutDown();
        }
        bool finished() noexcept
        {
            return _producer.eof();
        }

    };




    template<typename TSink, typename TItem, typename TSemaphore>
    struct Reduce
    {
    public:
        using item_type = TItem;
    private:
        TSink& _sink;
        std::vector<std::atomic<TItem*>> _tlsItems;
        std::thread _thread;
        std::atomic_bool _run;
        unsigned int _sleepMS;
        TSemaphore itemAvailableSemaphore;
        TSemaphore slotEmptySemaphore;

    public:
        Reduce(TSink& sink, unsigned int sleepMS = defaultSleepMS)
            : _sink(sink), _run(false), _sleepMS(sleepMS)
        {
            for (auto& item : _tlsItems)
                item.store(nullptr);  // fill initialization does not work for atomics
        }
        ~Reduce()
        {
            _run = false;
            if (_thread.joinable())
                _thread.join();
        }
        void start(const unsigned int numSlots)
        {
            _run = true;
            _tlsItems = std::vector<std::atomic<TItem*>>(numSlots);
            _thread = std::thread([this]()
            {
                std::unique_ptr<TItem> currentItem;
                bool nothingToDo = false;
                while (_run.load(std::memory_order_relaxed) || !nothingToDo)
                {
                    nothingToDo = true;
                    for (auto& item : _tlsItems)
                    {
                        if (item.load(std::memory_order_relaxed) != nullptr)
                        {
                            currentItem.reset(item.load(std::memory_order_relaxed));
                            item.store(nullptr, std::memory_order_release); // make the slot free again
                            if (!std::is_same<TSemaphore, NoSemaphore>::value)
                                slotEmptySemaphore.signal();
                            nothingToDo = false;

                            //std::this_thread::sleep_for(std::chrono::milliseconds(1));  // used for debuggin slow hd case
                            _sink(std::move(*currentItem));
                        }
                    }
                    if (nothingToDo)
                    {
                        //std::cout << std::this_thread::get_id() << "-4" << std::endl;
                        if (!std::is_same<TSemaphore, NoSemaphore>::value)
                        {
                            itemAvailableSemaphore.wait();
                        }
                        else
                            std::this_thread::sleep_for(std::chrono::milliseconds(_sleepMS));
                    }
                }
            });
        }
        void pushItem(std::unique_ptr<TItem> newItem)     // blocks until item could be added
        {
            while (true)
            {
                for (auto& item : _tlsItems)
                {
                    if (item.load(std::memory_order_relaxed) == nullptr)
                    {
                        TItem* temp = nullptr;
                        if (item.compare_exchange_strong(temp, newItem.get(), std::memory_order_acq_rel))  // acq_rel to make sure, that idle does not return true before all reads are written
                        {
                            newItem.release();
                            if (!std::is_same<TSemaphore, NoSemaphore>::value)
                                itemAvailableSemaphore.signal();
                            return;
                        }
                    }
                }
                //std::cout << std::this_thread::get_id() << "-3" << std::endl;
                if (!std::is_same<TSemaphore, NoSemaphore>::value)
                    slotEmptySemaphore.wait();
                else
                    std::this_thread::sleep_for(std::chrono::milliseconds(_sleepMS));
            }
        }
        void shutDown()
        {
            _run.store(false, std::memory_order_relaxed);
            if (!std::is_same<TSemaphore, NoSemaphore>::value)
                itemAvailableSemaphore.signal();
            if (_thread.joinable())
                _thread.join();
        }
        bool idle() noexcept
        {
            for (auto& item : _tlsItems)
                if (item.load(std::memory_order_acquire) != nullptr) // acq to make sure, that idle does not return true before all reads are written
                    return false;
            return true;
        }
    };

}