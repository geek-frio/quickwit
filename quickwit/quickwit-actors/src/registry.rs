// Copyright (C) 2022 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::collections::hash_map::Entry::{Occupied, Vacant};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use futures::future;
use serde::Serialize;
use tokio::sync::oneshot;

use crate::mailbox::WeakMailbox;
use crate::{Actor, Command, Mailbox};

struct TypedJsonObservable<A: Actor> {
    actor_instance_id: String,
    weak_mailbox: WeakMailbox<A>,
}

#[async_trait]
trait JsonObservable: Sync + Send {
    fn is_disconnected(&self) -> bool;
    fn any(&self) -> &dyn Any;
    fn actor_instance_id(&self) -> &str;
    async fn observe(&self) -> Option<serde_json::Value>;
}

#[async_trait]
impl<A: Actor> JsonObservable for TypedJsonObservable<A> {
    fn is_disconnected(&self) -> bool {
        self.weak_mailbox
            .upgrade()
            .map(|mailbox| mailbox.is_disconnected())
            .unwrap_or(true)
    }
    fn any(&self) -> &dyn Any {
        &self.weak_mailbox
    }
    fn actor_instance_id(&self) -> &str {
        self.actor_instance_id.as_str()
    }
    async fn observe(&self) -> Option<serde_json::Value> {
        let mailbox = self.weak_mailbox.upgrade()?;
        let (oneshot_tx, oneshot_rx) = oneshot::channel::<Box<dyn Any + Send>>();
        mailbox
            .send_message_with_high_priority(Command::Observe(oneshot_tx))
            .ok()?;
        let any_box = oneshot_rx.await.ok()?;
        let state: Box<<A as Actor>::ObservableState> =
            any_box.downcast::<A::ObservableState>().ok()?;
        serde_json::to_value(&*state).ok()
    }
}

#[derive(Default)]
pub struct Registry {
    actor_registry: ActorRegistry,
    singleton_registry: SingletonRegistry,
}

impl Registry {
     pub fn register_actor<A: Actor>(&self, mailbox: &Mailbox<A>) {
        self.actor_registry.register(mailbox);
    }

    pub async fn observe_actors(&self, timeout: Duration) -> Vec<ActorObservation> {
        self.actor_registry.observe(timeout).await
    }

    pub fn get_actor<A: Actor>(&self) -> Vec<Mailbox<A>> {
        self.actor_registry.get()
    }

    pub fn get_one_actor<A: Actor>(&self) -> Option<Mailbox<A>> {
        self.actor_registry.get_one()
    }

    pub fn get_singleton<T: Any + Sync + Send + 'static>(&self) -> Arc<T> {
        self.singleton_registry.get()
    }

    pub fn set_singleton<T: Any + Sync + Send + 'static>(&self, singleton: T) {
        self.singleton_registry.set(singleton)
    }
}

#[derive(Default)]
struct ActorRegistry {
    actors: RwLock<HashMap<TypeId, ActorRegistryForSpecificType>>,
}

struct ActorRegistryForSpecificType {
    type_name: &'static str,
    observables: Vec<Arc<dyn JsonObservable>>,
}

impl ActorRegistryForSpecificType {
    fn for_type<A>() -> ActorRegistryForSpecificType {
        ActorRegistryForSpecificType {
            type_name: std::any::type_name::<A>(),
            observables: Vec::new(),
        }
    }

    fn gc(&mut self) {
        let mut i = 0;
        while i < self.observables.len() {
            if self.observables[i].is_disconnected() {
                self.observables.swap_remove(i);
            } else {
                i += 1;
            }
        }
    }
}

#[derive(Serialize, Debug)]
pub struct ActorObservation {
    type_name: &'static str,
    instance_id: String,
    obs: Option<serde_json::Value>,
}

impl ActorRegistry {
    pub fn register<A: Actor>(&self, mailbox: &Mailbox<A>) {
        let typed_id = TypeId::of::<A>();
        let actor_instance_id = mailbox.actor_instance_id().to_string();
        let weak_mailbox = mailbox.downgrade();
        self.actors
            .write()
            .unwrap()
            .entry(typed_id)
            .or_insert_with(|| ActorRegistryForSpecificType::for_type::<A>())
            .observables
            .push(Arc::new(TypedJsonObservable {
                weak_mailbox,
                actor_instance_id,
            }));
    }

    pub async fn observe(&self, timeout: Duration) -> Vec<ActorObservation> {
        self.gc();
        let mut obs_futures = Vec::new();
        for registry_for_type in self.actors.read().unwrap().values() {
            for obs in &registry_for_type.observables {
                if obs.is_disconnected() {
                    continue;
                }
                let obs_clone = obs.clone();
                let type_name = registry_for_type.type_name;
                let instance_id = obs.actor_instance_id().to_string();
                obs_futures.push(async move {
                    let obs = tokio::time::timeout(timeout, obs_clone.observe())
                        .await
                        .unwrap_or(None);
                    ActorObservation {
                        type_name,
                        instance_id,
                        obs,
                    }
                });
            }
        }
        future::join_all(obs_futures.into_iter()).await
    }

    pub fn get<A: Actor>(&self) -> Vec<Mailbox<A>> {
        let mut lock = self.actors.write().unwrap();
        get_iter::<A>(&mut lock).collect()
    }

    pub fn get_one<A: Actor>(&self) -> Option<Mailbox<A>> {
        let mut lock = self.actors.write().unwrap();
        let opt = get_iter::<A>(&mut lock).next();
        opt
    }

    fn gc(&self) {
        for registry_for_type in self.actors.write().unwrap().values_mut() {
            registry_for_type.gc();
        }
    }
}

fn get_iter<A: Actor>(
    actors: &mut HashMap<TypeId, ActorRegistryForSpecificType>,
) -> impl Iterator<Item = Mailbox<A>> + '_ {
    let typed_id = TypeId::of::<A>();
    actors
        .get(&typed_id)
        .into_iter()
        .flat_map(|registry_for_type| {
            registry_for_type
                .observables
                .iter()
                .flat_map(|box_any| box_any.any().downcast_ref::<WeakMailbox<A>>())
                .flat_map(|weak_mailbox| weak_mailbox.upgrade())
        })
        .filter(|mailbox| !mailbox.is_disconnected())
}

#[derive(Default)]
pub struct SingletonRegistry {
    singletons: RwLock<HashMap<TypeId, Arc<dyn Any + Sync + Send + 'static>>>
}

impl SingletonRegistry {
    pub fn get<T: Any + Sync + Send + 'static>(&self) -> Arc<T> {
        let rlock = self.singletons.read().unwrap();
        if let Some(singleton_any) = rlock.get(&TypeId::of::<T>()) {
            singleton_any
                .clone()
                .downcast::<T>()
                .expect("Type does not match. This should never happen.")
                .clone()
        } else {
            panic!("Failed to find singleton for `{}`.", type_name::<T>());
        }
    }

    pub fn set<T: Any + Sync + Send + 'static>(&self, singleton: T) {
        let singleton_arc_any: Arc<dyn Any + Send + Sync + 'static> = Arc::new(singleton);
        let mut wlock = self.singletons.write().unwrap();
        let type_id = TypeId::of::<T>();
        match wlock.entry(type_id) {
            Occupied(_) => {
                panic!("Setting the same singleton several times for type `{}` is forbidden.", type_name::<T>());
            },
            Vacant(vacant_entry) => {
                vacant_entry.insert(singleton_arc_any);
            },
        }
    }
}


#[cfg(test)]
mod tests {
    use std::time::Duration;
    use super::SingletonRegistry;
    use crate::tests::PingReceiverActor;
    use crate::Universe;

    #[tokio::test]
    async fn test_actor_registry() {
        let test_actor = PingReceiverActor::default();
        let universe = Universe::new();
        let (_mailbox, _handle) = universe.spawn_builder().spawn(test_actor);
        let _actor_mailbox = universe.get_one::<PingReceiverActor>().unwrap();
    }

    #[tokio::test]
    async fn test_registry_killed_actor() {
        let test_actor = PingReceiverActor::default();
        let universe = Universe::new();
        let (_mailbox, handle) = universe.spawn_builder().spawn(test_actor);
        handle.kill().await;
        assert!(universe.get_one::<PingReceiverActor>().is_none());
    }

    #[tokio::test]
    async fn test_registry_last_mailbox_dropped_actor() {
        let test_actor = PingReceiverActor::default();
        let universe = Universe::new();
        let (mailbox, handle) = universe.spawn_builder().spawn(test_actor);
        drop(mailbox);
        handle.join().await;
        assert!(universe.get_one::<PingReceiverActor>().is_none());
    }

    #[tokio::test]
    async fn test_get_actor_states() {
        let test_actor = PingReceiverActor::default();
        let universe = Universe::new();
        let (_mailbox, _handle) = universe.spawn_builder().spawn(test_actor);
        let obs = universe.observe(Duration::from_millis(1000)).await;
        assert_eq!(obs.len(), 2);
    }

    struct TestConfig {
        count: usize
    }
    struct TestConfig2 {
        count: usize
    }

    #[test]
    fn test_singleton_registry() {
        let singleton_registry = SingletonRegistry::default();
        singleton_registry.set(TestConfig { count: 1 } );
        singleton_registry.set(TestConfig2 { count: 2 } );
        assert_eq!(singleton_registry.get::<TestConfig>().count, 1);
        assert_eq!(singleton_registry.get::<TestConfig2>().count, 2);
    }

    #[test]
    #[should_panic(expected="Failed to find singleton for `quickwit_actors::registry::tests::TestConfig`.")]
    fn test_singleton_registry_missing_should_panic() {
        let singleton_registry = SingletonRegistry::default();
        let _ = singleton_registry.get::<TestConfig>();
    }

    #[test]
    #[should_panic(expected="Setting the same singleton several times for type `quickwit_actors::registry::tests::TestConfig` is forbidden.")]
    fn test_singleton_registry_set_several_times_should_panic() {
        let singleton_registry = SingletonRegistry::default();
        singleton_registry.set(TestConfig { count: 1});
        singleton_registry.set(TestConfig { count: 1});
    }
}
